"""In-memory fakes implementing the Ask host ports, for service tests.

None of these import product or vendor code; they are built from the generic Ask +
LLM types alone, which is itself the demonstration that a host can wire the service
without importing any host identity, product stores, product query types, or a concrete
vendor SDK (AC2.1/AC2.2).
"""

from __future__ import annotations

from contextlib import AbstractContextManager, contextmanager
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence

from osprey.worker.lib.ask.contracts import (
    AskLimits,
    AskRequest,
    ContextSnapshot,
    Conversation,
    Principal,
    ResolvedModel,
)
from osprey.worker.lib.ask.errors import Forbidden, InvalidModel, LockUnavailable
from osprey.worker.lib.llm import (
    BaseLLMProvider,
    LLMMessage,
    LLMResponse,
    ToolCall,
    ToolDefinition,
    ToolRegistry,
    ToolResult,
)


class ScriptedProvider(BaseLLMProvider):
    """Returns a queued list of responses; records the messages seen on each call."""

    def __init__(self, responses: Sequence[LLMResponse]) -> None:
        self._responses = list(responses)
        self.calls: List[List[LLMMessage]] = []

    def chat(
        self,
        *,
        messages: Sequence[LLMMessage],
        system: Optional[str] = None,
        tools: Optional[Sequence[ToolDefinition]] = None,
        model: Optional[str] = None,
        max_tokens: Optional[int] = None,
        temperature: Optional[float] = None,
        **params: Any,
    ) -> LLMResponse:
        self.calls.append(list(messages))
        return self._responses[len(self.calls) - 1]


class RaisingProvider(BaseLLMProvider):
    """Raises on chat(); the message carries a secret to prove it never leaks."""

    def __init__(self, exc: Optional[BaseException] = None) -> None:
        self._exc = exc or RuntimeError('vendor stack trace secret: sk-DEADBEEF')

    def chat(
        self,
        *,
        messages: Sequence[LLMMessage],
        system: Optional[str] = None,
        tools: Optional[Sequence[ToolDefinition]] = None,
        model: Optional[str] = None,
        max_tokens: Optional[int] = None,
        temperature: Optional[float] = None,
        **params: Any,
    ) -> LLMResponse:
        raise self._exc


class AllowAllModelPolicy:
    def __init__(
        self,
        provider: BaseLLMProvider,
        model: Optional[str] = 'test-model',
        limits: Optional[AskLimits] = None,
    ) -> None:
        self._provider = provider
        self._model = model
        self._limits = limits

    def resolve(self, requested_model: Optional[str], principal: Principal) -> ResolvedModel:
        return ResolvedModel(model=requested_model or self._model, provider=self._provider, limits=self._limits)


class RejectingModelPolicy:
    def resolve(self, requested_model: Optional[str], principal: Principal) -> ResolvedModel:
        raise InvalidModel()


class NoProviderPolicy:
    def resolve(self, requested_model: Optional[str], principal: Principal) -> ResolvedModel:
        return ResolvedModel(model=requested_model, provider=None, limits=None)


class InMemoryConversationStore:
    def __init__(self) -> None:
        self._by_id: Dict[str, Conversation] = {}
        self._counter = 0
        self.appended: List[Dict[str, Any]] = []

    def add(self, conversation: Conversation) -> Conversation:
        self._by_id[conversation.id] = conversation
        return conversation

    def create(self, principal: Principal) -> Conversation:
        self._counter += 1
        conv = Conversation(id=f'conv-{self._counter}', principal_id=principal.id)
        self._by_id[conv.id] = conv
        return conv

    def load(self, conversation_id: str, principal: Principal) -> Optional[Conversation]:
        conv = self._by_id.get(conversation_id)
        if conv is None:
            return None
        if conv.principal_id != principal.id:
            raise Forbidden()
        return conv

    def append_assistant_turn(
        self,
        conversation: Conversation,
        request: AskRequest,
        response: LLMResponse,
        evidence: Sequence[Mapping[str, Any]],
    ) -> None:
        self.appended.append(
            {
                'conversation_id': conversation.id,
                'request': request,
                'response': response,
                'evidence': [dict(e) for e in evidence],
            }
        )


class InMemoryLock:
    def __init__(self) -> None:
        self.acquired = 0
        self.released = 0

    @contextmanager
    def _cm(self) -> Iterator[None]:
        self.acquired += 1
        try:
            yield
        finally:
            self.released += 1

    def acquire(self, conversation_id: str, principal: Principal) -> AbstractContextManager[None]:
        return self._cm()


class BlockedLock:
    def acquire(self, conversation_id: str, principal: Principal) -> AbstractContextManager[None]:
        raise LockUnavailable()


class StaticContextProvider:
    def __init__(
        self,
        snapshots: Optional[Mapping[str, ContextSnapshot]] = None,
        render_text: str = 'GROUNDING',
        render_error: bool = False,
    ) -> None:
        self._snapshots = dict(snapshots or {})
        self._render_text = render_text
        self._render_error = render_error

    def resolve(self, context_ref: str, principal: Principal) -> Optional[ContextSnapshot]:
        return self._snapshots.get(context_ref)

    def render(self, snapshot: ContextSnapshot) -> str:
        if self._render_error:
            raise RuntimeError('render blew up')
        return self._render_text


class RecordingToolRegistryFactory:
    def __init__(self, registry: ToolRegistry) -> None:
        self._registry = registry
        self.built_with_principal: Optional[Principal] = None
        self.built_with_snapshot: Optional[ContextSnapshot] = None

    def build(self, principal: Principal, snapshot: Optional[ContextSnapshot]) -> ToolRegistry:
        self.built_with_principal = principal
        self.built_with_snapshot = snapshot
        return self._registry


class RaisingToolRegistryFactory:
    def build(self, principal: Principal, snapshot: Optional[ContextSnapshot]) -> ToolRegistry:
        raise RuntimeError('tool factory blew up')


class TokenRedactor:
    """Replaces a secret token in text and (recursively) in structured payloads."""

    def __init__(self, secret: str, replacement: str = '[REDACTED]') -> None:
        self._secret = secret
        self._replacement = replacement

    def redact_text(self, text: str) -> str:
        return text.replace(self._secret, self._replacement)

    def redact_payload(self, value: Any) -> Any:
        if isinstance(value, str):
            return value.replace(self._secret, self._replacement)
        if isinstance(value, dict):
            return {k: self.redact_payload(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self.redact_payload(v) for v in value]
        return value


class RecordingAudit:
    def __init__(self, fail: bool = False) -> None:
        self.records: List[Dict[str, Any]] = []
        self._fail = fail

    def record(
        self,
        principal: Principal,
        conversation_id: str,
        turn_id: str,
        outcome: str,
        detail: Mapping[str, Any],
    ) -> None:
        if self._fail:
            raise RuntimeError('audit sink down')
        self.records.append({'outcome': outcome, 'detail': dict(detail), 'turn_id': turn_id})


class CustomEvidence:
    def normalize(self, tool_call: ToolCall, tool_result: ToolResult) -> Dict[str, Any]:
        return {'kind': 'custom', 'tool': tool_call.name, 'ok': not tool_result.is_error}
