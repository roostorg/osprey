import collections
from typing import Dict, Optional

import grpc
from osprey.worker.lib.ddtrace_utils import (
    baggage_propagator,
    current_span,
    get_baggage,
)

BAGGAGE_HEADER = 'baggage'


class BaggageInterceptor(
    grpc.UnaryUnaryClientInterceptor,  # type: ignore[misc]
    grpc.UnaryStreamClientInterceptor,  # type: ignore[misc]
    grpc.StreamUnaryClientInterceptor,  # type: ignore[misc]
    grpc.StreamStreamClientInterceptor,  # type: ignore[misc]
):
    """
    Propagates tracing "baggage" to downstream grpc services.

    This is only a partial, simple implementation of the W3C spec, skipping checks
    such as limits on the maximum number of baggage keys and the lengths of keys
    or values.

    All baggage keys and headers should already be strings, as Datadog expects
    tags to all be pre-stringified.

    For more info:
    https://opentelemetry.io/docs/reference/specification/baggage/api/
    """

    def __init__(self, baggage_header: Optional[str] = None, baggage: Optional[Dict[str, str]] = None):
        self._baggage_header = baggage_header or BAGGAGE_HEADER
        self._baggage = baggage or {}

    def intercept_unary_unary(self, continuation, client_call_details, request):
        return self._intercept_call(continuation, client_call_details, request)

    def intercept_unary_stream(self, continuation, client_call_details, request):
        return self._intercept_call(continuation, client_call_details, request)

    def intercept_stream_unary(self, continuation, client_call_details, request_iterator):
        return self._intercept_call(continuation, client_call_details, request_iterator)

    def intercept_stream_stream(self, continuation, client_call_details, request_iterator):
        return self._intercept_call(continuation, client_call_details, request_iterator)

    def _intercept_call(self, continuation, client_call_details, request_or_iterator):
        # The provided client_call_details may a) be a namedtuple and b) not yet have metadata
        # (i.e., client_call_details.metadata == None).
        # Since namedtuple fields are immutable, we need to provide our own namedtuple wrapper
        # in order to handle the case in which metadata is not yet set.
        metadata = list(client_call_details.metadata) if client_call_details.metadata else []

        # The baggage propagator expects a header-style dictionary but python's grpc metadata
        # is a list of tuples, so we need to convert
        baggage_headers = {}
        baggage_propagator.inject(get_baggage(current_span()), baggage_headers)
        metadata.extend([(k, v) for k, v in baggage_headers.items()])

        client_call_details = _ClientCallDetails(
            client_call_details.method, client_call_details.timeout, metadata, client_call_details.credentials
        )

        return continuation(client_call_details, request_or_iterator)


class _ClientCallDetails(
    collections.namedtuple('_ClientCallDetails', ('method', 'timeout', 'metadata', 'credentials')),
    grpc.ClientCallDetails,  # type: ignore[misc]
):
    pass
