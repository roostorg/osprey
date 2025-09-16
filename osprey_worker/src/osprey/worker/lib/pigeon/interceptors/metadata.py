import collections
from typing import Dict, Text

import grpc


class MetadataInterceptor(
    grpc.UnaryUnaryClientInterceptor,  # type: ignore[misc]
    grpc.UnaryStreamClientInterceptor,  # type: ignore[misc]
    grpc.StreamUnaryClientInterceptor,  # type: ignore[misc]
    grpc.StreamStreamClientInterceptor,  # type: ignore[misc]
):
    def __init__(self, metadata: Dict[Text, Text]):
        self._metadata = metadata

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

        for key, value in self._metadata.items():
            metadata.insert(0, (key, value))

        client_call_details = _ClientCallDetails(
            client_call_details.method, client_call_details.timeout, metadata, client_call_details.credentials
        )

        return continuation(client_call_details, request_or_iterator)


class _ClientCallDetails(
    collections.namedtuple('_ClientCallDetails', ('method', 'timeout', 'metadata', 'credentials')),
    grpc.ClientCallDetails,  # type: ignore[misc]
):
    pass
