"""AskService: the host-neutral turn orchestrator.

``run_turn`` is a generator. On the first ``next()`` it performs all pre-flight checks
(raising typed pre-flight errors before yielding anything), then streams a
deterministic, bounded sequence of events over the provider and a principal-scoped
tool registry, persists a successful turn, and yields exactly one terminal event.
The conversation lock is released on every terminal path, including client disconnect.
"""

from __future__ import annotations

import uuid
from typing import Any, Callable, Dict, Iterator, List, Optional

from osprey.worker.lib.ask.contracts import (
    AskEvent,
    AskLimits,
    AskRequest,
    ContextSnapshot,
    Conversation,
    Principal,
    ResolvedModel,
)
from osprey.worker.lib.ask.errors import (
    AskError,
    BudgetExceeded,
    Cancelled,
    Forbidden,
    InternalError,
    InvalidContext,
    InvalidRequest,
    ProviderError,
    ToolExecutionError,
    UnavailableProvider,
    to_public_payload,
)
from osprey.worker.lib.ask.ports import AskConfig
from osprey.worker.lib.llm import LLMMessage, LLMResponse, LLMUsage, ToolCall, ToolResult

_TRUNCATION_MARK = '…[truncated]'


def _new_id() -> str:
    return uuid.uuid4().hex


def _usage_payload(usage: Optional[LLMUsage]) -> Optional[Dict[str, int]]:
    if usage is None:
        return None
    return {
        'input_tokens': usage.input_tokens,
        'output_tokens': usage.output_tokens,
        'cache_read_tokens': usage.cache_read_tokens,
        'cache_write_tokens': usage.cache_write_tokens,
    }


class AskService:
    """Drives a single validated user turn to a bounded stream of :class:`AskEvent`."""

    def __init__(self, config: AskConfig) -> None:
        self._cfg = config

    def run_turn(
        self,
        request: AskRequest,
        principal: Principal,
        *,
        cancel: Optional[Callable[[], bool]] = None,
    ) -> Iterator[AskEvent]:
        cfg = self._cfg

        # ---- PRE-FLIGHT: raises before any yield; the transport maps to HTTP ----
        if not request.message or not request.message.strip():
            raise InvalidRequest()

        resolved: ResolvedModel = cfg.policy.resolve(request.model, principal)
        if resolved.provider is None:
            raise UnavailableProvider()
        provider = resolved.provider
        limits: AskLimits = resolved.limits or cfg.limits

        snapshot: Optional[ContextSnapshot] = None
        grounding: Optional[str] = None
        if request.context_ref is not None:
            context_provider = cfg.context
            if context_provider is None:
                raise InvalidContext()
            snapshot = context_provider.resolve(request.context_ref, principal)
            if snapshot is None:
                raise InvalidContext()
            try:
                grounding = context_provider.render(snapshot)[: limits.max_context_chars]
            except AskError:
                raise
            except Exception as exc:  # render failure is a pre-flight invalid-context, not mid-stream
                raise InvalidContext() from exc

        conversation = self._load_or_create(request.conversation_id, principal)
        lock_cm = cfg.lock.acquire(conversation.id, principal)  # may raise LockUnavailable

        # ---- STREAM: lock held; released on success, error, and GeneratorExit ----
        turn_id = _new_id()
        cid = conversation.id
        with lock_cm:
            try:
                yield AskEvent('conversation_started', {'conversation_id': cid, 'turn_id': turn_id}, cid, turn_id)
                try:
                    registry = cfg.tools.build(principal, snapshot)  # principal-scoped tools
                except AskError:
                    raise  # a factory may reject with e.g. Forbidden; keep its code
                except Exception as exc:
                    raise ToolExecutionError() from exc  # framework tool failure => terminal tool_error
                messages = self._build_messages(conversation, request, grounding, limits)
                evidence_log: List[Dict[str, Any]] = []
                final: Optional[LLMResponse] = None

                for i in range(limits.max_tool_iterations):
                    self._raise_if_cancelled(cancel)
                    try:
                        resp = provider.chat(
                            messages=messages,
                            system=cfg.system_prompt,
                            tools=registry.definitions(),
                            model=resolved.model,
                            max_tokens=limits.max_output_tokens,
                        )
                    except Exception as exc:
                        raise ProviderError() from exc  # never leak vendor text
                    self._raise_if_cancelled(cancel)  # after provider return, before side effects

                    if not resp.tool_calls:
                        final = resp
                        break
                    if i == limits.max_tool_iterations - 1:
                        raise BudgetExceeded()  # do not dispatch tools we can't feed back

                    messages.append(
                        LLMMessage(role='assistant', content=resp.text or None, tool_calls=list(resp.tool_calls))
                    )
                    results: List[ToolResult] = []
                    for tc in resp.tool_calls:  # deterministic order
                        self._raise_if_cancelled(cancel)
                        yield AskEvent('tool_call', self._tool_call_payload(tc), cid, turn_id)
                        try:
                            tr = registry.dispatch(tc)  # handler errors are bounded is_error results
                        except Exception as exc:
                            raise ToolExecutionError() from exc  # framework tool failure is terminal
                        results.append(tr)
                        evidence = self._evidence_payload(tc, tr, limits)
                        evidence_log.append(evidence)
                        yield AskEvent('query_result', evidence, cid, turn_id)
                    messages.append(LLMMessage(role='tool', tool_results=results))
                else:
                    raise BudgetExceeded()  # exhausted iterations without a final answer

                assert final is not None  # guaranteed: loop either breaks with final or raises
                self._raise_if_cancelled(cancel)
                # Build + validate all terminal data BEFORE committing, so a budget failure
                # never persists a "successful" turn whose terminal event cannot ship.
                text = self._finalize_text(final.text, limits)
                done_payload = {'conversation_id': cid, 'turn_id': turn_id, 'usage': _usage_payload(final.usage)}
                yield AskEvent('assistant_message', {'text': text}, cid, turn_id)
                cfg.conversations.append_assistant_turn(conversation, request, final, evidence_log)
                self._safe_audit(principal, cid, turn_id, 'done', {})
                yield AskEvent('done', done_payload, cid, turn_id)
            except AskError as err:
                self._safe_audit(principal, cid, turn_id, 'error', {'code': err.code})
                yield self._error_event(err, cid, turn_id)  # streaming began => terminal error event
            except Exception:
                self._safe_audit(principal, cid, turn_id, 'error', {'code': 'internal'})
                yield self._error_event(InternalError(), cid, turn_id)

    # --- helpers -------------------------------------------------------------

    def _load_or_create(self, conversation_id: Optional[str], principal: Principal) -> Conversation:
        if conversation_id is None:
            return self._cfg.conversations.create(principal)
        conv = self._cfg.conversations.load(conversation_id, principal)
        if conv is None:
            # A provided id that is unknown or not owned is treated as forbidden, so we
            # never leak the existence of another principal's conversation.
            raise Forbidden()
        return conv

    def _build_messages(
        self,
        conversation: Conversation,
        request: AskRequest,
        grounding: Optional[str],
        limits: AskLimits,
    ) -> List[LLMMessage]:
        history = list(conversation.messages)[-limits.max_history_messages :]
        messages: List[LLMMessage] = []
        if grounding:
            messages.append(LLMMessage(role='system', content=grounding))
        messages.extend(history)
        messages.append(LLMMessage(role='user', content=request.message))
        return messages

    def _finalize_text(self, text: str, limits: AskLimits) -> str:
        redactor = self._cfg.redactor
        if redactor is not None:
            text = redactor.redact_text(text)
        if len(text) > limits.max_output_chars:
            raise BudgetExceeded()
        return text

    def _tool_call_payload(self, tc: ToolCall) -> Dict[str, Any]:
        return {'id': tc.id, 'name': tc.name, 'arguments': self._redact_value(tc.arguments)}

    def _evidence_payload(self, tc: ToolCall, tr: ToolResult, limits: AskLimits) -> Dict[str, Any]:
        cfg = self._cfg
        if cfg.evidence is not None:
            payload: Dict[str, Any] = dict(cfg.evidence.normalize(tc, tr))
        else:
            payload = {
                'tool_call_id': tr.tool_call_id,
                'name': tc.name,
                'is_error': tr.is_error,
                'content': tr.content,
            }
        payload = self._redact_value(payload)
        return self._truncate_strings(payload, limits.max_evidence_chars)

    def _redact_value(self, value: Any) -> Any:
        redactor = self._cfg.redactor
        if redactor is None:
            return value
        return redactor.redact_payload(value)

    @staticmethod
    def _truncate_strings(value: Any, limit: int) -> Any:
        if isinstance(value, str):
            return value if len(value) <= limit else value[:limit] + _TRUNCATION_MARK
        if isinstance(value, dict):
            return {k: AskService._truncate_strings(v, limit) for k, v in value.items()}
        if isinstance(value, list):
            return [AskService._truncate_strings(v, limit) for v in value]
        return value

    def _error_event(self, err: AskError, cid: str, turn_id: str) -> AskEvent:
        return AskEvent('error', to_public_payload(err), cid, turn_id)

    def _raise_if_cancelled(self, cancel: Optional[Callable[[], bool]]) -> None:
        if cancel is not None and cancel():
            raise Cancelled()

    def _safe_audit(self, principal: Principal, cid: str, turn_id: str, outcome: str, detail: Dict[str, Any]) -> None:
        audit = self._cfg.audit
        if audit is None:
            return
        try:
            audit.record(principal, cid, turn_id, outcome, detail)
        except Exception:  # audit is best-effort and must never corrupt the stream
            pass
