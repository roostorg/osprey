"""AskService: deterministic, bounded, safe turns.

Covers osprey-ask-ai AC2.1-AC2.5 and AC3.1-AC3.5. Tests drain ``run_turn`` and assert
on the event-type sequence and payloads. These live under ``lib/`` and therefore run
through the Docker harness (``run-tests.sh``); the autouse Postgres fixture applies but
is not otherwise exercised here.
"""

from __future__ import annotations

from typing import Any, List, Optional

import pytest
from osprey.worker.lib.ask import (
    AskConfig,
    AskEvent,
    AskLimits,
    AskRequest,
    AskService,
    Forbidden,
    InvalidContext,
    InvalidModel,
    InvalidRequest,
    LockUnavailable,
    Principal,
    UnavailableProvider,
)
from osprey.worker.lib.ask.contracts import ContextSnapshot, Conversation
from osprey.worker.lib.ask.tests.fakes import (
    AllowAllModelPolicy,
    BlockedLock,
    CustomEvidence,
    InMemoryConversationStore,
    InMemoryLock,
    NoProviderPolicy,
    RaisingProvider,
    RaisingToolRegistryFactory,
    RecordingAudit,
    RecordingToolRegistryFactory,
    RejectingModelPolicy,
    ScriptedProvider,
    StaticContextProvider,
    TokenRedactor,
)
from osprey.worker.lib.llm import LLMMessage, LLMResponse, Tool, ToolCall, ToolRegistry

PRINCIPAL = Principal(id='u1', email='u1@example.com')
OTHER = Principal(id='u2', email='u2@example.com')


def _text(text: str) -> LLMResponse:
    return LLMResponse(text=text)


def _tools(*calls: ToolCall) -> LLMResponse:
    return LLMResponse(text='', tool_calls=list(calls))


def _registry(*tools: Tool) -> ToolRegistry:
    reg = ToolRegistry()
    for t in tools:
        reg.register(t)
    return reg


def _ok_tool(name: str = 'lookup', result: Any = None) -> Tool:
    def handler(**kwargs: Any) -> Any:
        return {'result': 'ok'} if result is None else result

    return Tool(name=name, description='d', handler=handler)


def _raising_tool(name: str = 'boom', message: str = 'kaboom') -> Tool:
    def handler(**kwargs: Any) -> Any:
        raise ValueError(message)

    return Tool(name=name, description='d', handler=handler)


def _build(
    provider: Any,
    *,
    registry: Optional[ToolRegistry] = None,
    store: Optional[InMemoryConversationStore] = None,
    lock: Optional[Any] = None,
    context: Optional[Any] = None,
    redactor: Optional[Any] = None,
    audit: Optional[Any] = None,
    evidence: Optional[Any] = None,
    limits: Optional[AskLimits] = None,
    tool_factory: Optional[Any] = None,
) -> Any:
    store = store or InMemoryConversationStore()
    lock = lock or InMemoryLock()
    factory = tool_factory or RecordingToolRegistryFactory(registry or ToolRegistry())
    cfg = AskConfig(
        policy=AllowAllModelPolicy(provider),
        conversations=store,
        lock=lock,
        tools=factory,
        limits=limits or AskLimits(),
        context=context,
        redactor=redactor,
        audit=audit,
        evidence=evidence,
    )
    return AskService(cfg), store, lock, factory


def _drain(
    service: AskService, request: AskRequest, principal: Principal = PRINCIPAL, cancel: Any = None
) -> List[AskEvent]:
    return list(service.run_turn(request, principal, cancel=cancel))


def _types(events: List[AskEvent]) -> List[str]:
    return [e.type for e in events]


# --- AC3.1 / AC3.2 ---------------------------------------------------------


def test_happy_path_one_tool_call_sequence_and_persist():
    call = ToolCall(id='c1', name='lookup', arguments={'q': 7})
    provider = ScriptedProvider([_tools(call), _text('final answer')])
    service, store, lock, _ = _build(provider, registry=_registry(_ok_tool('lookup', result={'v': 1})))
    events = _drain(service, AskRequest(message='hi'))
    assert _types(events) == ['conversation_started', 'tool_call', 'query_result', 'assistant_message', 'done']
    assert _types(events).count('done') == 1
    assert not any(e.type == 'error' for e in events)
    assert events[3].payload['text'] == 'final answer'
    # tool result fed back: the 2nd provider call ends with a 'tool' message
    assert len(provider.calls) == 2
    assert provider.calls[1][-1].role == 'tool'
    # persisted exactly once with the final response
    assert len(store.appended) == 1
    assert store.appended[0]['response'].text == 'final answer'
    assert lock.acquired == 1 and lock.released == 1


def test_new_conversation_id_flows_through_events_and_persist():
    provider = ScriptedProvider([_text('ok')])
    service, store, _, _ = _build(provider)
    events = _drain(service, AskRequest(message='hi'))
    cid = events[0].conversation_id
    assert events[0].type == 'conversation_started'
    assert cid and cid.startswith('conv-')
    assert all(e.conversation_id == cid for e in events)
    assert store.appended[0]['conversation_id'] == cid


# --- AC3.5 (multiple tool calls; iteration budget) -------------------------


def test_multiple_tool_calls_ordered_deterministically():
    c1 = ToolCall(id='c1', name='a', arguments={})
    c2 = ToolCall(id='c2', name='b', arguments={})
    provider = ScriptedProvider([_tools(c1, c2), _text('done')])
    service, _, _, _ = _build(provider, registry=_registry(_ok_tool('a', 'ra'), _ok_tool('b', 'rb')))
    events = _drain(service, AskRequest(message='hi'))
    assert _types(events) == [
        'conversation_started',
        'tool_call',
        'query_result',
        'tool_call',
        'query_result',
        'assistant_message',
        'done',
    ]
    assert [e.payload['name'] for e in events if e.type == 'tool_call'] == ['a', 'b']


def test_tool_iteration_budget_does_not_dispatch_on_final_iteration():
    call = ToolCall(id='c', name='counter', arguments={})
    provider = ScriptedProvider([_tools(call)] * 10)
    dispatched = {'n': 0}

    def handler(**kwargs: Any) -> Any:
        dispatched['n'] += 1
        return 'x'

    reg = _registry(Tool(name='counter', description='d', handler=handler))
    service, store, lock, _ = _build(provider, registry=reg, limits=AskLimits(max_tool_iterations=3))
    events = _drain(service, AskRequest(message='hi'))
    assert events[-1].type == 'error'
    assert events[-1].payload['code'] == 'budget_exceeded'
    # iters 0 and 1 dispatch; iter 2 is the final allowed call -> raise before dispatch
    assert dispatched['n'] == 2
    assert store.appended == []
    assert lock.acquired == 1 and lock.released == 1


# --- AC2.5 (bounds) --------------------------------------------------------


def test_output_char_budget_exceeded_blocks_persist():
    provider = ScriptedProvider([_text('x' * 100)])
    service, store, _, _ = _build(provider, limits=AskLimits(max_output_chars=10))
    events = _drain(service, AskRequest(message='hi'))
    assert _types(events) == ['conversation_started', 'error']
    assert events[-1].payload['code'] == 'budget_exceeded'
    assert store.appended == []


def test_history_truncated_to_limit():
    provider = ScriptedProvider([_text('ok')])
    store = InMemoryConversationStore()
    store.add(
        Conversation(
            id='c1',
            principal_id=PRINCIPAL.id,
            messages=[LLMMessage(role='user', content=f'm{i}') for i in range(50)],
        )
    )
    service, _, _, _ = _build(provider, store=store, limits=AskLimits(max_history_messages=5))
    _drain(service, AskRequest(message='new', conversation_id='c1'))
    seen = provider.calls[0]
    assert len(seen) == 6  # 5 history + 1 new user message
    assert seen[-1].content == 'new'


def test_context_grounding_truncated_and_tools_scoped_to_principal():
    snap = ContextSnapshot(ref='r1')
    context = StaticContextProvider(snapshots={'r1': snap}, render_text='G' * 100)
    provider = ScriptedProvider([_text('ok')])
    factory = RecordingToolRegistryFactory(ToolRegistry())
    service, _, _, _ = _build(provider, context=context, limits=AskLimits(max_context_chars=10), tool_factory=factory)
    _drain(service, AskRequest(message='hi', context_ref='r1'))
    seen = provider.calls[0]
    assert seen[0].role == 'system'
    assert seen[0].content == 'G' * 10  # truncated grounding
    assert factory.built_with_principal is PRINCIPAL  # server principal, not context
    assert factory.built_with_snapshot is snap


def test_evidence_content_truncated():
    call = ToolCall(id='c', name='lookup', arguments={})
    provider = ScriptedProvider([_tools(call), _text('ok')])
    service, _, _, _ = _build(
        provider, registry=_registry(_ok_tool('lookup', result='y' * 100)), limits=AskLimits(max_evidence_chars=10)
    )
    events = _drain(service, AskRequest(message='hi'))
    content = [e for e in events if e.type == 'query_result'][0].payload['content']
    assert content.startswith('y' * 10)
    assert content.endswith('…[truncated]')


# --- AC2.4 (pre-flight rejections; raised before any event) ----------------


def test_empty_message_rejected_preflight():
    provider = ScriptedProvider([_text('x')])
    service, _, lock, _ = _build(provider)
    with pytest.raises(InvalidRequest):
        _drain(service, AskRequest(message='   '))
    assert provider.calls == []
    assert lock.acquired == 0


def test_invalid_model_rejected_preflight():
    cfg = AskConfig(
        policy=RejectingModelPolicy(),
        conversations=InMemoryConversationStore(),
        lock=InMemoryLock(),
        tools=RecordingToolRegistryFactory(ToolRegistry()),
    )
    with pytest.raises(InvalidModel):
        list(AskService(cfg).run_turn(AskRequest(message='hi'), PRINCIPAL))


def test_unavailable_provider_rejected_preflight():
    cfg = AskConfig(
        policy=NoProviderPolicy(),
        conversations=InMemoryConversationStore(),
        lock=InMemoryLock(),
        tools=RecordingToolRegistryFactory(ToolRegistry()),
    )
    with pytest.raises(UnavailableProvider):
        list(AskService(cfg).run_turn(AskRequest(message='hi'), PRINCIPAL))


def test_unknown_context_ref_rejected_preflight():
    provider = ScriptedProvider([_text('x')])
    service, _, lock, _ = _build(provider, context=StaticContextProvider(snapshots={}))
    with pytest.raises(InvalidContext):
        _drain(service, AskRequest(message='hi', context_ref='missing'))
    assert lock.acquired == 0


def test_context_ref_without_provider_rejected_preflight():
    provider = ScriptedProvider([_text('x')])
    service, _, _, _ = _build(provider, context=None)
    with pytest.raises(InvalidContext):
        _drain(service, AskRequest(message='hi', context_ref='r1'))


def test_context_render_failure_is_preflight_invalid_context():
    provider = ScriptedProvider([_text('x')])
    context = StaticContextProvider(snapshots={'r1': ContextSnapshot(ref='r1')}, render_error=True)
    service, _, lock, _ = _build(provider, context=context)
    with pytest.raises(InvalidContext):
        _drain(service, AskRequest(message='hi', context_ref='r1'))
    assert provider.calls == []
    assert lock.acquired == 0


def test_foreign_conversation_forbidden_preflight():
    provider = ScriptedProvider([_text('x')])
    store = InMemoryConversationStore()
    store.add(Conversation(id='c1', principal_id=OTHER.id))
    service, _, lock, _ = _build(provider, store=store)
    with pytest.raises(Forbidden):
        _drain(service, AskRequest(message='hi', conversation_id='c1'))
    assert lock.acquired == 0


def test_unknown_conversation_id_forbidden_preflight():
    provider = ScriptedProvider([_text('x')])
    service, _, _, _ = _build(provider)
    with pytest.raises(Forbidden):
        _drain(service, AskRequest(message='hi', conversation_id='does-not-exist'))


def test_lock_unavailable_rejected_preflight():
    provider = ScriptedProvider([_text('x')])
    service, _, _, _ = _build(provider, lock=BlockedLock())
    with pytest.raises(LockUnavailable):
        _drain(service, AskRequest(message='hi'))
    assert provider.calls == []


# --- AC3.3 / AC3.4 (failures are terminal + safe; lock released; no persist) --


def test_provider_error_is_safe_terminal_and_releases_lock():
    provider = RaisingProvider()  # message contains 'sk-DEADBEEF'
    service, store, lock, _ = _build(provider)
    events = _drain(service, AskRequest(message='hi'))
    assert _types(events) == ['conversation_started', 'error']
    assert events[-1].payload['code'] == 'provider_error'
    assert 'sk-DEADBEEF' not in events[-1].payload['message']
    assert 'RuntimeError' not in events[-1].payload['message']
    assert store.appended == []
    assert lock.acquired == 1 and lock.released == 1


def test_tool_handler_exception_is_nonterminal_query_result():
    call = ToolCall(id='c', name='boom', arguments={})
    provider = ScriptedProvider([_tools(call), _text('recovered')])
    service, store, _, _ = _build(provider, registry=_registry(_raising_tool('boom', 'kaboom')))
    events = _drain(service, AskRequest(message='hi'))
    assert _types(events) == ['conversation_started', 'tool_call', 'query_result', 'assistant_message', 'done']
    qr = [e for e in events if e.type == 'query_result'][0]
    assert qr.payload['is_error'] is True
    assert 'kaboom' in qr.payload['content']
    assert len(store.appended) == 1


def test_tool_factory_failure_is_terminal_tool_error():
    provider = ScriptedProvider([_text('never')])
    service, store, lock, _ = _build(provider, tool_factory=RaisingToolRegistryFactory())
    events = _drain(service, AskRequest(message='hi'))
    assert events[-1].type == 'error'
    assert events[-1].payload['code'] == 'tool_error'
    assert store.appended == []
    assert lock.acquired == 1 and lock.released == 1


def test_cancellation_is_terminal_and_skips_persist():
    call = ToolCall(id='c', name='lookup', arguments={})
    provider = ScriptedProvider([_tools(call), _text('final')])
    state = {'n': 0}

    def cancel() -> bool:
        state['n'] += 1
        return state['n'] >= 2  # allow the first check, cancel on the next

    service, store, lock, _ = _build(provider, registry=_registry(_ok_tool('lookup')))
    events = _drain(service, AskRequest(message='hi'), cancel=cancel)
    assert events[-1].type == 'error'
    assert events[-1].payload['code'] == 'cancelled'
    assert store.appended == []
    assert lock.acquired == 1 and lock.released == 1


def test_generator_exit_releases_lock_without_persist():
    call = ToolCall(id='c', name='lookup', arguments={})
    provider = ScriptedProvider([_tools(call), _text('final')])
    service, store, lock, _ = _build(provider, registry=_registry(_ok_tool('lookup')))
    gen = service.run_turn(AskRequest(message='hi'), PRINCIPAL)
    assert next(gen).type == 'conversation_started'
    gen.close()  # simulate client disconnect
    assert lock.acquired == 1 and lock.released == 1
    assert store.appended == []


# --- AC4.4 boundary (structured redaction) + AC2.1 (adapters) --------------


def test_structured_redaction_of_args_evidence_and_text():
    secret = 'sk-SECRET'
    call = ToolCall(id='c', name='lookup', arguments={'token': secret, 'nested': {'k': secret}})
    provider = ScriptedProvider([_tools(call), _text(f'answer {secret}')])
    service, _, _, _ = _build(
        provider, registry=_registry(_raising_tool('lookup', f'failed with {secret}')), redactor=TokenRedactor(secret)
    )
    events = _drain(service, AskRequest(message='hi'))
    tc = [e for e in events if e.type == 'tool_call'][0]
    qr = [e for e in events if e.type == 'query_result'][0]
    am = [e for e in events if e.type == 'assistant_message'][0]
    assert secret not in str(tc.payload) and '[REDACTED]' in str(tc.payload)
    assert secret not in str(qr.payload)
    assert secret not in am.payload['text']


def test_custom_evidence_normalizer_used():
    call = ToolCall(id='c', name='lookup', arguments={})
    provider = ScriptedProvider([_tools(call), _text('ok')])
    service, _, _, _ = _build(provider, registry=_registry(_ok_tool('lookup')), evidence=CustomEvidence())
    events = _drain(service, AskRequest(message='hi'))
    qr = [e for e in events if e.type == 'query_result'][0]
    assert qr.payload == {'kind': 'custom', 'tool': 'lookup', 'ok': True}


def test_audit_failure_does_not_break_stream():
    provider = ScriptedProvider([_text('ok')])
    service, store, _, _ = _build(provider, audit=RecordingAudit(fail=True))
    events = _drain(service, AskRequest(message='hi'))
    assert events[-1].type == 'done'
    assert len(store.appended) == 1
