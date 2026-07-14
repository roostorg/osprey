"""Reusable Flask blueprint factory exposing an AskService as versioned SSE.

Host-configurable: the host supplies a ``service_factory`` (which builds an
:class:`AskService` from its concrete adapters) and a ``principal_provider`` (which
derives the server-authenticated principal -- never from the request body), and
chooses the URL. All pre-flight work runs inside a safe boundary so an unexpected
host-adapter failure becomes a safe JSON 500 rather than a raw stack trace. Once
streaming begins, exactly one terminal event is emitted. Nothing here imports host
identity or product-specific types.
"""

from __future__ import annotations

from typing import Any, Callable, Generator, Iterator, Optional, Tuple

import pydantic
from flask import Blueprint, Response, jsonify, request, stream_with_context
from osprey.worker.lib.ask import AskEvent, AskRequest, AskService, Principal
from osprey.worker.lib.ask.errors import (
    AskError,
    InternalError,
    InvalidRequest,
    SerializationError,
    to_public_payload,
)
from osprey.worker.ui_api.osprey.lib.ask.sse import format_sse

ServiceFactory = Callable[[], AskService]
PrincipalProvider = Callable[[], Principal]

_TERMINAL = ('done', 'error')


class AskRequestModel(pydantic.BaseModel):
    """HTTP request body schema. Strict strings so a JSON number/bool is rejected."""

    message: pydantic.StrictStr
    conversation_id: Optional[pydantic.StrictStr] = None
    model: Optional[pydantic.StrictStr] = None
    context_ref: Optional[pydantic.StrictStr] = None

    class Config:
        extra = 'forbid'


def _http_error(err: AskError) -> Tuple[Any, int]:
    return jsonify({'error': to_public_payload(err)}), err.http_status


def create_ask_blueprint(
    *,
    name: str,
    service_factory: ServiceFactory,
    principal_provider: PrincipalProvider,
    url: str = '/ask',
) -> Blueprint:
    """Build a blueprint with a single POST route that streams Ask events as SSE."""
    bp = Blueprint(name, __name__)

    @bp.route(url, methods=['POST'])
    def ask() -> Any:
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return _http_error(InvalidRequest())
        try:
            model_in = AskRequestModel(**body)
        except pydantic.ValidationError:
            return _http_error(InvalidRequest())
        if not model_in.message.strip():
            return _http_error(InvalidRequest())

        ask_request = AskRequest(
            message=model_in.message,
            conversation_id=model_in.conversation_id,
            model=model_in.model,
            context_ref=model_in.context_ref,
        )

        # Safe pre-flight boundary: principal derivation, service construction, generator
        # creation, and the pre-flight next() must never leak a raw 500 / HTML page.
        cancelled = {'v': False}
        gen: Optional[Generator[AskEvent, None, None]] = None
        try:
            principal = principal_provider()
            service = service_factory()
            gen = service.run_turn(ask_request, principal, cancel=lambda: cancelled['v'])
            first = next(gen)
        except AskError as err:
            if gen is not None:
                gen.close()
            return _http_error(err)
        except StopIteration:
            if gen is not None:
                gen.close()
            return _http_error(InternalError())
        except Exception:
            if gen is not None:
                gen.close()
            return _http_error(InternalError())

        active_gen = gen
        assert active_gen is not None  # reached only when next(gen) succeeded

        def stream() -> Iterator[str]:
            terminal_seen = first.type in _TERMINAL
            try:
                yield format_sse(first)  # conversation_started
                for event in active_gen:
                    terminal_seen = terminal_seen or event.type in _TERMINAL
                    yield format_sse(event)
            except SerializationError as err:
                if not terminal_seen:
                    terminal_seen = True
                    yield format_sse(AskEvent('error', to_public_payload(err), first.conversation_id, first.turn_id))
            except GeneratorExit:
                cancelled['v'] = True
                active_gen.close()  # service finally releases the lock
                raise
            except Exception:
                if not terminal_seen:  # terminal guard: never a second terminal after done
                    terminal_seen = True
                    yield format_sse(
                        AskEvent('error', to_public_payload(InternalError()), first.conversation_id, first.turn_id)
                    )
            finally:
                active_gen.close()

        resp = Response(stream_with_context(stream()), mimetype='text/event-stream')
        resp.headers['Cache-Control'] = 'no-cache'
        resp.headers['X-Accel-Buffering'] = 'no'
        return resp

    return bp
