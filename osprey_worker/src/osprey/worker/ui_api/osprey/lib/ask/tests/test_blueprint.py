"""Ask SSE blueprint: framing, versioning, pre-flight errors, safe failures, termination.

Covers osprey-ask-ai AC4.1-AC4.5. A minimal Flask app registers the blueprint with a
fake ``service_factory`` (an AskService built from the Phase 2 port fakes) and a fake
``principal_provider``. These live under ui_api, whose conftest applies the autouse
Postgres fixture, so they run through the Docker harness (run-tests.sh).
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Tuple

from flask import Flask
from osprey.worker.lib.ask import AskConfig, AskLimits, AskService, Principal
from osprey.worker.lib.ask.contracts import Conversation
from osprey.worker.lib.ask.tests.fakes import (
    AllowAllModelPolicy,
    BlockedLock,
    InMemoryConversationStore,
    InMemoryLock,
    NoProviderPolicy,
    RaisingProvider,
    RejectingModelPolicy,
    ScriptedProvider,
)
from osprey.worker.lib.llm import LLMResponse, Tool, ToolCall, ToolRegistry
from osprey.worker.ui_api.osprey.lib.ask.blueprint import create_ask_blueprint

PRINCIPAL = Principal(id='u1', email='u1@example.com')


class RecordingRegistryFactory:
    def __init__(self, registry: ToolRegistry = None) -> None:  # type: ignore[assignment]
        self.builds = 0
        self._reg = registry if registry is not None else ToolRegistry()

    def build(self, principal: Principal, snapshot: Any) -> ToolRegistry:
        self.builds += 1
        return self._reg


class BadEvidence:
    def normalize(self, tool_call: ToolCall, tool_result: Any) -> Dict[str, Any]:
        return {'bad': object()}  # not JSON-serializable -> forces a serialization error


class RaisingExitLock:
    def __init__(self) -> None:
        self.acquired = 0

    @contextmanager
    def _cm(self) -> Iterator[None]:
        self.acquired += 1
        yield
        raise RuntimeError('lock __exit__ blew up after the turn completed')

    def acquire(self, conversation_id: str, principal: Principal) -> Any:
        return self._cm()


def _text(t: str) -> LLMResponse:
    return LLMResponse(text=t)


def _tools(*calls: ToolCall) -> LLMResponse:
    return LLMResponse(text='', tool_calls=list(calls))


def _factory(provider: Any, *, store=None, lock=None, tools=None, policy=None, limits=None, evidence=None):
    store = store or InMemoryConversationStore()
    lock = lock or InMemoryLock()
    tools = tools or RecordingRegistryFactory()
    policy = policy or AllowAllModelPolicy(provider)

    def make() -> AskService:
        return AskService(
            AskConfig(
                policy=policy,
                conversations=store,
                lock=lock,
                tools=tools,
                limits=limits or AskLimits(),
                evidence=evidence,
            )
        )

    return make, store, lock, tools


def _app(service_factory: Any, principal_provider: Any = lambda: PRINCIPAL) -> Flask:
    app = Flask(__name__)
    app.register_blueprint(
        create_ask_blueprint(
            name='ask', service_factory=service_factory, principal_provider=principal_provider, url='/ask'
        )
    )
    return app


def _post(app: Flask, payload: Any) -> Any:
    return app.test_client().post('/ask', json=payload)


def _parse_sse(body: str) -> List[Tuple[str, Dict[str, Any]]]:
    frames: List[Tuple[str, Dict[str, Any]]] = []
    for block in body.split('\n\n'):
        if not block.strip():
            continue
        name = ''
        data = ''
        for line in block.split('\n'):
            if line.startswith('event: '):
                name = line[len('event: ') :]
            elif line.startswith('data: '):
                data += line[len('data: ') :]
        frames.append((name, json.loads(data)))
    return frames


def _names(frames: List[Tuple[str, Dict[str, Any]]]) -> List[str]:
    return [n for n, _ in frames]


# --- AC4.1 / AC4.2 ---------------------------------------------------------


def test_happy_path_content_type_framing_versioning():
    make, store, lock, factory = _factory(ScriptedProvider([_text('hello')]))
    resp = _post(_app(make), {'message': 'hi'})
    assert resp.status_code == 200
    assert resp.mimetype == 'text/event-stream'
    frames = _parse_sse(resp.get_data(as_text=True))
    assert _names(frames) == ['conversation_started', 'assistant_message', 'done']
    for name, data in frames:
        assert data['version'] == 1
        assert data['type'] == name
    assert len(store.appended) == 1
    assert lock.acquired == 1 and lock.released == 1  # resources released through the route


# --- AC4.3 (appropriate errors, no stream, no tools) -----------------------


def test_missing_message_400_no_tools():
    make, _, _, factory = _factory(ScriptedProvider([_text('x')]))
    resp = _post(_app(make), {})
    assert resp.status_code == 400
    assert resp.get_json()['error']['code'] == 'invalid_request'
    assert factory.builds == 0


def test_wrong_type_message_400():
    make, _, _, _ = _factory(ScriptedProvider([_text('x')]))
    resp = _post(_app(make), {'message': 123})
    assert resp.status_code == 400
    assert resp.get_json()['error']['code'] == 'invalid_request'


def test_extra_field_400():
    make, _, _, _ = _factory(ScriptedProvider([_text('x')]))
    resp = _post(_app(make), {'message': 'hi', 'nope': 1})
    assert resp.status_code == 400


def test_non_json_body_400():
    make, _, _, _ = _factory(ScriptedProvider([_text('x')]))
    resp = _app(make).test_client().post('/ask', data='not json', content_type='text/plain')
    assert resp.status_code == 400


def test_unavailable_provider_503():
    make, _, _, factory = _factory(ScriptedProvider([_text('x')]), policy=NoProviderPolicy())
    resp = _post(_app(make), {'message': 'hi'})
    assert resp.status_code == 503
    assert resp.get_json()['error']['code'] == 'unavailable_provider'
    assert factory.builds == 0


def test_invalid_model_400():
    make, _, _, _ = _factory(ScriptedProvider([_text('x')]), policy=RejectingModelPolicy())
    resp = _post(_app(make), {'message': 'hi'})
    assert resp.status_code == 400
    assert resp.get_json()['error']['code'] == 'invalid_model'


def test_foreign_conversation_403():
    store = InMemoryConversationStore()
    store.add(Conversation(id='c1', principal_id='someone-else'))
    make, _, _, _ = _factory(ScriptedProvider([_text('x')]), store=store)
    resp = _post(_app(make), {'message': 'hi', 'conversation_id': 'c1'})
    assert resp.status_code == 403
    assert resp.get_json()['error']['code'] == 'forbidden'


def test_lock_unavailable_409():
    make, _, _, _ = _factory(ScriptedProvider([_text('x')]), lock=BlockedLock())
    resp = _post(_app(make), {'message': 'hi'})
    assert resp.status_code == 409
    assert resp.get_json()['error']['code'] == 'lock_unavailable'


def test_throwing_service_factory_safe_500():
    def make() -> AskService:
        raise RuntimeError('boom secret sk-XYZ')

    resp = _post(_app(make), {'message': 'hi'})
    assert resp.status_code == 500
    assert resp.get_json()['error']['code'] == 'internal'
    assert 'sk-XYZ' not in resp.get_data(as_text=True)


def test_throwing_principal_provider_safe_500():
    make, _, _, _ = _factory(ScriptedProvider([_text('x')]))

    def principal() -> Principal:
        raise RuntimeError('identity secret')

    resp = _app(make, principal_provider=principal).test_client().post('/ask', json={'message': 'hi'})
    assert resp.status_code == 500
    assert resp.get_json()['error']['code'] == 'internal'


# --- AC4.4 (safe failures) + AC4.5 (exactly one terminal) ------------------


def test_provider_failure_single_safe_error_frame():
    make, store, lock, _ = _factory(RaisingProvider())  # message carries 'sk-DEADBEEF'
    resp = _post(_app(make), {'message': 'hi'})
    assert resp.status_code == 200  # the stream started
    body = resp.get_data(as_text=True)
    assert 'sk-DEADBEEF' not in body
    frames = _parse_sse(body)
    assert _names(frames) == ['conversation_started', 'error']
    assert frames[-1][1]['payload']['code'] == 'provider_error'
    assert store.appended == []
    assert lock.acquired == 1 and lock.released == 1


def test_serialization_failure_yields_terminal_error():
    reg = ToolRegistry()
    reg.register(Tool(name='lookup', description='d', handler=lambda **k: 'ok'))
    call = ToolCall(id='c', name='lookup', arguments={})
    make, _, lock, _ = _factory(
        ScriptedProvider([_tools(call), _text('final')]),
        tools=RecordingRegistryFactory(reg),
        evidence=BadEvidence(),
    )
    frames = _parse_sse(_post(_app(make), {'message': 'hi'}).get_data(as_text=True))
    names = _names(frames)
    assert names[0] == 'conversation_started'
    assert names[-1] == 'error'
    assert frames[-1][1]['payload']['code'] == 'serialization_error'
    assert names.count('error') == 1
    assert lock.acquired == 1 and lock.released == 1


def test_terminal_guard_no_second_terminal_after_done():
    lock = RaisingExitLock()
    make, store, _, _ = _factory(ScriptedProvider([_text('final')]), lock=lock)
    frames = _parse_sse(_post(_app(make), {'message': 'hi'}).get_data(as_text=True))
    names = _names(frames)
    assert names == ['conversation_started', 'assistant_message', 'done']
    assert names.count('done') == 1
    assert 'error' not in names
    assert lock.acquired == 1
