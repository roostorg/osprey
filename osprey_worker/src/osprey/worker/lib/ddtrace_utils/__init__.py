import functools
import sys
from typing import Any, Callable, Dict, Optional, TypeVar

import ddtrace
from ddtrace._trace.pin import Pin
from ddtrace.trace import Span
from flask import Flask
from osprey.worker.lib.ddtrace_utils.instrumentation.flask.middleware import TraceMiddleware
from osprey.worker.lib.ddtrace_utils.internal.baggage import Baggage

# Re-export globals
from osprey.worker.lib.ddtrace_utils.internal.globals import baggage_manager, baggage_propagator  # noqa: F401

F = TypeVar('F', bound=Callable[..., Any])


def init_app(app: Flask, service_name: str, redact_entity_ids_in_urls: bool = False) -> None:
    app.after_request(after_flask_request)

    TraceMiddleware(
        app,
        ddtrace.tracer,
        service=service_name,
        distributed_tracing=True,
        redact_entity_ids_in_urls=redact_entity_ids_in_urls,
    )


def after_flask_request(response):
    from osprey.worker.lib.osprey_logging import request_metadata

    span = current_span()
    trace_id = f'{span.trace_id}:{span.span_id}'

    request_metadata.update(trace_id=trace_id)

    return response


def trace(
    name: str,
    service: Optional[str] = None,
    resource: Optional[str] = None,
    span_type: Optional[str] = None,
    tags: Optional[Dict[str, str]] = None,
) -> Span:
    span = ddtrace.tracer.trace(name, service, resource, span_type) or _noop_span()
    if tags:
        span.set_tags(tags)
    return span


def trace_wrap(*args, **kwargs) -> Callable[[F], F]:
    return ddtrace.tracer.wrap(*args, **kwargs)


def current_span() -> Span:
    return ddtrace.tracer.current_span() or _noop_span()


def pin_override(cluster: Any, service: Optional[str], tags: Optional[Dict[str, str]] = None) -> None:
    Pin.override(cluster, service=service, tags=tags)


def get_baggage(span: Span) -> Baggage:
    return baggage_manager.get_baggage(span)


def noop_span() -> Span:
    return _noop_span()


# python-upgrade-compat: Span has a different type signature in the later version of ddtrace
if sys.version_info < (3, 11):

    def _noop_span() -> Span:
        return Span(None, 'no-op')

else:

    def _noop_span() -> Span:
        return Span('no-op')


def with_tracing_context(ctx=None):
    """Decoractor to wraps a function with a new tracing context (defaults to None)"""

    def tracing_context_wrapper(func):
        @functools.wraps(func)
        def wrapper_decorator(*args, **kwargs):
            old_ctx = ddtrace.tracer.context_provider.active()
            ddtrace.tracer.context_provider.activate(ctx)
            try:
                value = func(*args, **kwargs)
            finally:
                ddtrace.tracer.context_provider.activate(old_ctx)
            return value

        return wrapper_decorator

    return tracing_context_wrapper
