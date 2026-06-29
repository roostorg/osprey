# Copied from https://github.com/DataDog/dd-trace-py/blob/v0.39.0/ddtrace/contrib/flask/middleware.py
# In order to fix status_code bug
import re
import sys
from collections.abc import Callable
from typing import Any

import flask.templating
from ddtrace import Span, Tracer
from ddtrace.constants import ERROR_MSG, ERROR_TYPE
from ddtrace.ext import SpanTypes, http
from ddtrace.internal import compat
from ddtrace.internal.logger import get_logger
from ddtrace.propagation.http import HTTPPropagator
from flask import Flask, g, request, signals
from osprey.worker.lib.ddtrace_utils.constants import SpanAttributes
from osprey.worker.lib.ddtrace_utils.internal.globals import baggage_manager, baggage_propagator

log = get_logger(__name__)

SPAN_NAME = 'flask.request'

# Matches path segments that are uint64s, which likely represent snowflakes.
ENTITY_ID_IN_PATH_REGEX = re.compile(r'(?:(?<=^)|(?<=\/))\d{10,}(?=$|\/)')


def _redact_entity_ids_in_path(path: str) -> str:
    """
    Returns the URL or path with numeric segments replaced with a single `?`.
    """
    return ENTITY_ID_IN_PATH_REGEX.sub('?', path)


class TraceMiddleware:
    def __init__(
        self,
        app: Flask,
        tracer: Tracer,
        service: str = 'flask',
        use_signals: bool = True,
        distributed_tracing: bool = False,
        redact_entity_ids_in_urls: bool = False,
    ):
        self.app = app
        log.debug('flask: initializing trace middleware')

        # Attach settings to the inner application middleware. This is required if double
        # instrumentation happens (i.e. `ddtrace-run` with `TraceMiddleware`). In that
        # case, `ddtrace-run` instruments the application, but then users code is unable
        # to update settings such as `distributed_tracing` flag. This step can be removed
        # when the `Config` object is used
        self.app._tracer = tracer
        self.app._service = service
        self.app._use_distributed_tracing = distributed_tracing
        self.use_signals = use_signals
        self.redact_entity_ids_in_urls = redact_entity_ids_in_urls

        # safe-guard to avoid double instrumentation
        if getattr(app, '__dd_instrumentation', False):
            return
        setattr(app, '__dd_instrumentation', True)

        # Install hooks which time requests.
        self.app.before_request(self._before_request)
        self.app.after_request(self._after_request)
        self.app.teardown_request(self._teardown_request)

        # Add exception handling signals. This will annotate exceptions that
        # are caught and handled in custom user code.
        # See https://github.com/DataDog/dd-trace-py/issues/390
        if use_signals and not signals.signals_available:
            log.debug(_blinker_not_installed_msg)
        self.use_signals = use_signals and signals.signals_available
        timing_signals = {
            'got_request_exception': self._request_exception,
        }
        self._receivers: list[Callable[[Any, Any], None]] = []
        if self.use_signals and _signals_exist(timing_signals):
            self._connect(timing_signals)

        _patch_render(tracer)

    def _connect(self, signal_to_handler: dict[str, Callable[[Any, Any], None]]) -> bool:
        connected = True
        for name, handler in signal_to_handler.items():
            s = getattr(signals, name, None)
            if not s:
                connected = False
                log.warning('trying to instrument missing signal %s', name)
                continue
            # we should connect to the signal without using weak references
            # otherwise they will be garbage collected and our handlers
            # will be disconnected after the first call; for more details check:
            # https://github.com/jek/blinker/blob/207446f2d97/blinker/base.py#L106-L108
            s.connect(handler, sender=self.app, weak=False)
            self._receivers.append(handler)
        return connected

    def _before_request(self) -> None:
        """Starts tracing the current request and stores it in the global
        request object.
        """
        self._start_span()

    def _after_request(self, response: flask.Response) -> flask.Response:
        """Runs after the server can process a response."""
        try:
            self._process_response(response)
        except Exception:
            log.debug('flask: error tracing response', exc_info=True)
        return response

    def _teardown_request(self, exception: Exception | None) -> None:
        """Runs at the end of a request. If there's an unhandled exception, it
        will be passed in.
        """
        # when we teardown the span, ensure we have a clean slate.
        span = getattr(g, 'flask_datadog_span', None)
        setattr(g, 'flask_datadog_span', None)
        if not span:
            return

        try:
            self._finish_span(span, exception=exception)
        except Exception:
            log.debug('flask: error finishing span', exc_info=True)

    def _start_span(self) -> None:
        if self.app._use_distributed_tracing:
            propagator = HTTPPropagator()

            # Only need to activate the new context if something was propagated
            # ddtrace has inconsistent typing within itself, request.headers implements
            # all the expected methods
            context = propagator.extract(request.headers)
            if context.trace_id:
                self.app._tracer.context_provider.activate(context)
        try:
            span = self.app._tracer.trace(SPAN_NAME, service=self.app._service, span_type=SpanTypes.WEB)

            baggage = baggage_propagator.extract(request.headers)  # type: ignore[arg-type]
            baggage_manager.store_baggage_items(span, baggage, False)

            local_baggage = {}
            dd_service: str | None = request.environ.get('DD_SERVICE')
            role: str | None = request.environ.get('role')
            service: str | None = self.app._service or dd_service or role

            if span.name:
                local_baggage[SpanAttributes.OPERATION] = span.name

            if service:
                local_baggage[SpanAttributes.SERVICE] = service

            if request:
                api_version = request.environ.get('API_VERSION')

                if api_version:
                    span.set_tag('api.version', api_version)
                    local_baggage[SpanAttributes.API_VERSION] = api_version

                if request.endpoint:
                    local_baggage[SpanAttributes.RESOURCE] = compat.to_unicode(request.endpoint).lower()

                if request.method:
                    local_baggage[SpanAttributes.METHOD] = request.method

                url = request.base_url

                path_info = str(request.environ.get('PATH_INFO', ''))
                raw_path_info = str(request.environ.get('RAW_PATH_INFO', ''))

                path = raw_path_info or path_info

                if path:
                    if self.redact_entity_ids_in_urls:
                        path = _redact_entity_ids_in_path(path)

                    # NOTE: Our flask spans currently have an `http.path_group` tag, the origin
                    # of which I couldn't find. This tag uses the modified `path_info` that
                    # strips out the verison.
                    # We intentionally want to preserve that tag but specify a (new) `http.path`
                    # tag that uses the unstripped path.
                    span.set_tag('http.path', path)

                if url:
                    if path != path_info:
                        # Set the URL path to match what we're using for `http.path` above.
                        url = url.replace(path_info, path)

                    # Override the default wsgi `http.url` since that one will have stripped
                    # out the request version
                    span.set_tag('http.url', url)
                    local_baggage[SpanAttributes.URL] = url

                timeout = request.headers.get('x-envoy-expected-rq-timeout-ms')
                if timeout:
                    span.set_tag('envoy-timeout-ms', int(timeout))
                content_length = request.content_length
                if content_length:
                    span.set_tag('http.content_length', int(content_length))

            baggage_manager.store_baggage_items(span, local_baggage)

            g.flask_datadog_span = span
        except Exception:
            log.debug('flask: error tracing request', exc_info=True)

    def _process_response(self, response: flask.Response) -> None:
        span = getattr(g, 'flask_datadog_span', None)
        if not (span and span.sampled):
            return

        code: str | int = response.status_code if response else ''
        # Sometimes the status code is a good old <enum HTTPStatus.NO_CONTENT: 204> which makes ddtrace barf unless
        # we manually coerce, since it tries to be smart and sets the tag to the string value of the enum, but this
        # makes the trace agent drop the heckin trace.
        try:
            # This would probably always work but I am spooked that they do the same thing below
            code = int(code)
        except Exception:
            code = 0
        span.set_tag(http.STATUS_CODE, code)

    def _request_exception(self, *args: Any, **kwargs: Any) -> None:
        exception = kwargs.get('exception', None)
        span = getattr(g, 'flask_datadog_span', None)
        if span and exception:
            _set_error_on_span(span, exception)

    def _finish_span(self, span: Span, exception: Exception | None = None) -> None:
        if not span or not span.sampled:
            return

        code: int | str = span.get_tag(http.STATUS_CODE) or 0
        try:
            code = int(code)
        except Exception:
            code = 0

        if exception:
            # if the request has already had a code set, don't override it.
            code = code or 500
            _set_error_on_span(span, exception)

        # the endpoint that matched the request is None if an exception
        # happened so we fallback to a common resource
        span.error = 0 if code < 500 else 1

        # the request isn't guaranteed to exist here, so only use it carefully.
        method = ''
        endpoint = ''
        url = ''
        if request:
            method = request.method
            endpoint = request.endpoint or str(code)
            url = request.base_url or ''

        if url:
            url = compat.to_unicode(url)
            if self.redact_entity_ids_in_urls:
                url = _redact_entity_ids_in_path(url)

        # Let users specify their own resource in middleware if they so desire.
        # See case https://github.com/DataDog/dd-trace-py/issues/353
        if span.resource == SPAN_NAME:
            resource = endpoint or str(code)
            span.resource = compat.to_unicode(resource).lower()

        span.set_tag(http.URL, url)
        span.set_tag(http.STATUS_CODE, code)
        span.set_tag(http.METHOD, method)

        span.finish()


def _set_error_on_span(span: Span, exception: Exception) -> None:
    # The 3 next lines might not be strictly required, since `set_traceback`
    # also get the exception from the sys.exc_info (and fill the error meta).
    # Since we aren't sure it always work/for insuring no BC break, keep
    # these lines which get overridden anyway.
    span.set_tag(ERROR_TYPE, type(exception))
    span.set_tag(ERROR_MSG, exception)
    # The provided `exception` object doesn't have a stack trace attached,
    # so attach the stack trace with `set_traceback`.
    span.set_traceback()


def _patch_render(tracer: Tracer) -> None:
    """patch flask's render template methods with the given tracer."""
    # fall back to patching  global method
    _render = flask.templating._render  # type: ignore[attr-defined]

    def _traced_render(template: Any, context: Any, app: Flask) -> Any:
        # python-upgrade-compat: span_type has a different type in the later version of ddtrace
        if sys.version_info < (3, 11):
            span_type = SpanTypes.TEMPLATE.value
        else:
            span_type = SpanTypes.TEMPLATE
        with tracer.trace('flask.template', span_type=span_type) as span:
            span.set_tag('flask.template', template.name or 'string')
            return _render(template, context, app)

    flask.templating._render = _traced_render  # type: ignore[attr-defined]


def _signals_exist(names: dict[str, Callable[[Any, Any], None]]) -> bool:
    """Return true if all of the given signals exist in this version of flask."""
    return all(getattr(signals, n, False) for n in names)


_blinker_not_installed_msg = 'please install blinker to use flask signals. http://flask.pocoo.org/docs/0.11/signals/'
