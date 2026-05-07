import asyncio
import copy
import itertools
import logging
import weakref
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from time import time_ns
from typing import Any, Dict, Generic, List, Optional, Set, Tuple, Type, TypeVar, Union, cast

import grpc
import grpc.aio
from ddtrace.constants import ERROR_MSG
from ddtrace.contrib.grpc.constants import GRPC_STATUS_CODE_KEY
from ddtrace.ext.http import STATUS_CODE
from ddtrace.span import Span
from google.protobuf.message import Message
from osprey.worker.lib.ddtrace_utils import current_span, noop_span, pin_override, trace
from osprey.worker.lib.discovery.exceptions import ServiceUnavailable
from osprey.worker.lib.discovery.service import Service
# String constant matching osprey.worker.lib.discovery.service_watcher.DOWN
# Defined locally to avoid importing service_watcher which pulls in gevent.
DOWN = 'down'

from osprey.async_worker.lib.discovery.async_directory import AsyncDirectory
from osprey.worker.lib.instruments import metrics
from osprey.worker.lib.pigeon.exceptions import InvalidRoutingValueException, NoResponsesException, RPCException
from osprey.worker.lib.pigeon.interceptors.baggage import BaggageInterceptor
from osprey.worker.lib.pigeon.interceptors.metadata import MetadataInterceptor
from typing_extensions import TypedDict

from osprey.async_worker.lib.pigeon.skip_rate_limit import skip_rate_limit_context

logger = logging.getLogger(__name__)

T = TypeVar('T')


class ServiceDefinition(TypedDict):
    address: str
    ip: Optional[str]
    port: int


# This mapping follows the recommendation at https://cloud.google.com/apis/design/errors#handling_errors.
# We *could* use `http.HTTPStatus` here, but it's arguably more readable without the abstraction in this case
# and wouldn't be complete anyway as it doesn't contain anything for 499.
_GRPC_HTTP_CODE_TRANSLATIONS = {
    grpc.StatusCode.OK: 200,
    grpc.StatusCode.CANCELLED: 499,
    grpc.StatusCode.UNKNOWN: 500,
    grpc.StatusCode.INVALID_ARGUMENT: 400,
    grpc.StatusCode.DEADLINE_EXCEEDED: 504,
    grpc.StatusCode.NOT_FOUND: 404,
    grpc.StatusCode.ALREADY_EXISTS: 409,
    grpc.StatusCode.PERMISSION_DENIED: 403,
    grpc.StatusCode.RESOURCE_EXHAUSTED: 429,
    grpc.StatusCode.FAILED_PRECONDITION: 400,
    grpc.StatusCode.ABORTED: 409,
    grpc.StatusCode.OUT_OF_RANGE: 400,
    grpc.StatusCode.UNIMPLEMENTED: 501,
    grpc.StatusCode.INTERNAL: 500,
    grpc.StatusCode.UNAVAILABLE: 503,
    grpc.StatusCode.DATA_LOSS: 500,
    grpc.StatusCode.UNAUTHENTICATED: 401,
}
_GRPC_CODE_FALLBACK = grpc.StatusCode.UNKNOWN

# This is the name of the span that the pigeon client uses.  The Service RPC Guard uses this name
# to avoid creating a new span for the grpc request and just adds info to the existing guard span
PIGEON_REQUEST_SPAN_NAME = 'pigeon.request'


class RoutingType:
    CHUNKED = 1
    SCALAR = 2
    ROUND_ROBIN = 3
    ENVOY = 4

    ALL = {CHUNKED, SCALAR, ROUND_ROBIN, ENVOY}


class RoutedClient(Generic[T]):
    def __init__(
        self,
        service_name,
        read_timeout,
        stub_cls: Type[T],
        request_field=None,
        request_field_routing_value_transform=None,
        routing_type=RoutingType.CHUNKED,
        secondaries=1,
        pool_size=200,
        chunk_size=250,
        secure_etcd=False,
        metadata=None,
        interceptors=None,
        grpc_options=None,
        acceptable_duration_ms=None,
        baggage_header=None,
        baggage=None,
        envoy_endpoint: Optional[ServiceDefinition] = None,
        use_peer_service_name=False,
        default_retry_policy: Optional['RetryPolicy'] = None,
    ):
        self._service_name = service_name
        self._peer_service = f'{service_name}-client' if use_peer_service_name else service_name
        self._stub_cls: Type[T] = stub_cls
        self._request_field = request_field
        self._request_field_routing_value_transform = request_field_routing_value_transform
        self._routing_type = routing_type
        self._open_channels: Dict[Tuple[Tuple[str, Optional[str]], int], weakref.ReferenceType[grpc.aio.Channel]] = {}
        self._clients: Dict[Tuple[Tuple[str, Optional[str]], int], T] = {}
        self._secondaries = secondaries
        self._chunk_size = chunk_size
        self._semaphore = asyncio.Semaphore(pool_size)
        self._read_timeout = read_timeout
        grpc_options = {'grpc.keepalive_time_ms': 300_000, **(grpc_options or dict())}
        self._grpc_options = list(grpc_options.items())
        self._connect_eagerly = False
        self._acceptable_duration_ms: Optional[int] = acceptable_duration_ms
        self._default_retry_policy: Optional['RetryPolicy'] = default_retry_policy
        self._interceptors: List[Any] = [BaggageInterceptor(baggage_header=baggage_header, baggage=baggage)]

        if metadata:
            self._interceptors.append(MetadataInterceptor(metadata))

        if interceptors:
            self._interceptors.extend(interceptors)

        if RoutingType.ENVOY == self._routing_type:
            if envoy_endpoint is None:
                raise ValueError('RoutingType.ENVOY could not create service')
            self._envoy_service = Service(
                name=service_name,
                address=envoy_endpoint['address'],
                port=int(envoy_endpoint['port']),
                ports={'grpc': envoy_endpoint['port']},
                metadata={},
                ip=envoy_endpoint.get('ip'),
            )
        else:
            self._async_directory = AsyncDirectory.instance(secure=secure_etcd)
            self._service_watcher = self._async_directory.get_watcher(service_name)
            self._service_watcher_initialized = False
            self._handle_service_change_fn = self._handle_service_change
            self._service_watcher.add_lazy_listener(self._handle_service_change_fn)

    def __getattr__(self, method_name) -> 'AsyncUnaryUnaryRpcCallable[T, Any, Any]':
        return AsyncUnaryUnaryRpcCallable(service_name=self._service_name, method_name=method_name, client=self)

    @property
    def acceptable_duration_ms(self) -> Optional[int]:
        return self._acceptable_duration_ms

    async def _ensure_watcher_initialized(self) -> None:
        """Lazily initialize the async service watcher on first use.

        Must be called from an async context (inside the event loop).
        Safe to call multiple times — only initializes once.
        """
        if hasattr(self, '_service_watcher_initialized') and not self._service_watcher_initialized:
            # Set flag before awaiting to prevent concurrent coroutines from
            # double-initializing. Reset on failure so the next call retries.
            self._service_watcher_initialized = True
            try:
                await self._service_watcher.ensure_initialized()
                logger.info(
                    'async service watcher initialized for %s: %d instances',
                    self._service_name,
                    len(self._service_watcher._instances),
                )
            except Exception:
                self._service_watcher_initialized = False
                logger.exception('failed to initialize async service watcher for %s', self._service_name)
                raise

    async def request(
        self,
        method_name: str,
        message: Message,
        request_field: Optional[str] = None,
        routing_type: Optional[int] = None,
        timeout: Optional[float] = None,
        metadata: Optional[List[Tuple[str, str]]] = None,
        instances_to_skip: int = 0,
    ):
        await self._ensure_watcher_initialized()
        routing_type = routing_type if routing_type is not None else self._routing_type
        request_field = request_field if request_field is not None else self._request_field
        timeout = timeout or self._read_timeout

        if skip_rate_limit_context.skip:
            metadata = metadata or []
            metadata.append(('skip-rate-limit', 'true'))

        if routing_type == RoutingType.CHUNKED:
            return await self._chunked_request(method_name, message, request_field, timeout=timeout, metadata=metadata)
        else:
            return await self._request(
                method_name,
                message,
                request_field,
                routing_type,
                timeout=timeout,
                metadata=metadata,
                instances_to_skip=instances_to_skip,
            )

    async def _chunked_request(
        self,
        method_name: str,
        message: Message,
        request_field: str,
        timeout: Optional[float] = None,
        metadata: Optional[List[Tuple[str, str]]] = None,
    ):
        """Call a remote service concurrently. Route based on a routing key."""
        calls = self._generate_routed_calls(request_field, message)
        num_chunks = len(calls)

        current_span().set_tag('num_request_chunks', str(num_chunks))

        # Avoid spawning a task for singular calls.
        if num_chunks == 1:
            return await self._do_routed_request(
                method_name, message, request_field, timeout, metadata, next(iter(calls.items()))
            )

        async def _bounded_request(item):
            async with self._semaphore:
                return await self._do_routed_request(method_name, message, request_field, timeout, metadata, item)

        responses = await asyncio.gather(*[_bounded_request(item) for item in calls.items()])

        final_response = None
        for response in responses:
            if response:
                if not final_response:
                    final_response = response
                else:
                    final_response.MergeFrom(response)

        if final_response is None:
            raise NoResponsesException()

        return final_response

    async def _request(
        self,
        method_name: str,
        message: Message,
        request_field: str,
        routing_type: int,
        timeout: Optional[float] = None,
        metadata: Optional[List[Tuple[str, str]]] = None,
        instances_to_skip: int = 0,
    ):
        """Request from a remote service."""
        service = self._select_service(message, request_field, routing_type, instances_to_skip)
        client = self._get_client(service)
        method = getattr(client, method_name)
        return await method(message, timeout=timeout, metadata=metadata)

    async def _do_routed_request(
        self,
        method_name: str,
        message_template: Message,
        request_field: str,
        timeout: Optional[float],
        metadata: Optional[List[Tuple[str, str]]],
        service_and_routing_values,
    ) -> Optional[Message]:
        """Request from remote service."""
        (service, routing_values) = service_and_routing_values
        with maybe_start_span('pigeon.routed_request', self._peer_service, method_name):
            span = current_span()
            set_protocol(span)
            client = self._get_client(service)
            method = getattr(client, method_name)
            routing_values_iter = iter(routing_values)
            final_response = None
            while True:
                routing_values_chunk = list(itertools.islice(routing_values_iter, 0, self._chunk_size))
                if not routing_values_chunk:
                    break

                next_message = _make_message(message_template, request_field, routing_values, routing_values_chunk)
                response = await method(next_message, timeout=timeout, metadata=metadata)
                if not final_response:
                    final_response = response.__class__()
                final_response.MergeFrom(response)

            return final_response

    def _select_service(
        self, message: Message, request_field: str, routing_type: int, instances_to_skip: int = 0
    ) -> Service:
        """Select a service based on the client routing."""
        if routing_type == RoutingType.SCALAR:
            routing_value = getattr(message, request_field)
            if self._request_field_routing_value_transform:
                routing_value = self._request_field_routing_value_transform(routing_value)

            return self._service_watcher.select(
                routing_value, secondaries=self._secondaries, instances_to_skip=instances_to_skip
            )
        elif routing_type == RoutingType.ROUND_ROBIN:
            return self._service_watcher.select(secondaries=self._secondaries)
        elif routing_type == RoutingType.ENVOY:
            return self._envoy_service
        else:
            # RoutingType.CHUNKED is handled in another code path.
            raise RuntimeError(f'RoutingType {routing_type} not supported')

    def _generate_routed_calls(self, request_field, message):
        """Generate the routed calls. XXX: Modifies message by removing its routed values."""
        field = getattr(message, request_field)
        if not field:
            raise InvalidRoutingValueException(request_field, field)

        request_field_routing_value_transform = self._request_field_routing_value_transform

        if isinstance(field, Mapping):
            groups = defaultdict(dict)
            for key, value in field.items():
                if request_field_routing_value_transform:
                    key = request_field_routing_value_transform(key)

                groups[self._service_watcher.select(key, self._secondaries)][key] = value
        else:
            groups = defaultdict(list)
            for value in field:
                if request_field_routing_value_transform:
                    value = request_field_routing_value_transform(value)

                groups[self._service_watcher.select(value, self._secondaries)].append(value)

        message.ClearField(request_field)
        return groups

    def connect_eagerly(self) -> None:
        self._connect_eagerly = True
        for service in self._service_watcher.select_all():
            self._get_client(service)

    def _handle_service_change(self, status: str, service: Service) -> None:
        # Clean up the client when the service marks itself as DOWN.
        if status != DOWN:
            return
        service_key = self._get_service_key(service)
        self._cleanup_client(service_key)

    def _cleanup_client(self, service_key):
        if service_key in self._open_channels:
            channel = self._open_channels.pop(service_key)()
            if channel is not None and hasattr(channel, 'close'):
                # grpc.aio channels have an async close, but we fire-and-forget here
                # since this is called from a sync callback.
                try:
                    asyncio.get_event_loop().create_task(channel.close())
                except RuntimeError:
                    pass  # No running event loop (shutdown or non-main thread)
        if service_key in self._clients:
            del self._clients[service_key]

    @staticmethod
    def _get_service_key(service: Service) -> Tuple[Tuple[str, Optional[str]], int]:
        return (service.connection_key, service.grpc_port)

    def _get_client(self, service: Service) -> T:
        """Get the client for the service"""
        key = self._get_service_key(service)
        try:
            return self._clients[key]
        except KeyError:
            addr_port = f'{service.connection_address}:{service.grpc_port}'

            # grpc.aio requires async interceptors (grpc.aio.UnaryUnaryClientInterceptor).
            # The BaggageInterceptor/MetadataInterceptor are sync interceptors that only
            # work with ENVOY routing (pre-created channels). For discovered services
            # (SCALAR/ROUND_ROBIN), create channels without interceptors.
            pin_override(grpc.Channel, f'{service.name}-grpc-client')
            channel = grpc.aio.insecure_channel(addr_port, options=self._grpc_options)
            self._open_channels[key] = weakref.ref(channel)

            pin_override(grpc.Channel, None)

            client = self._stub_cls(channel)  # type: ignore
            self._clients[key] = client
            return client


def _make_message(
    message_template: Message,
    request_field: str,
    routing_values: Union[List[Any], Dict[Any, Any]],
    routing_values_chunk: List[Any],
):
    message = copy.copy(message_template)
    field = getattr(message, request_field)
    if isinstance(field, Mapping):
        for key in routing_values_chunk:
            value = routing_values[key]
            if isinstance(value, Message):
                field[key].CopyFrom(value)
            else:
                field[key] = value  # type: ignore
    else:
        field.extend(routing_values_chunk)
    return message


Request = TypeVar('Request')
Response = TypeVar('Response')


class RetryPolicy(TypedDict):
    retryable_grpc_status_codes: Set[grpc.StatusCode]
    max_secondaries_to_retry: int


@dataclass
class AsyncUnaryUnaryRpcCallable(Generic[T, Request, Response]):
    service_name: str
    method_name: str
    client: RoutedClient[T]

    async def __call__(
        self,
        message: Request,
        request_field: Optional[str] = None,
        routing_type: Optional[int] = None,
        timeout: Optional[float] = None,
        acceptable_duration_ms: Optional[int] = None,
        metadata: Optional[List[Tuple[str, str]]] = None,
        retry_policy: Optional[RetryPolicy] = None,
    ) -> Response:
        retry_policy = retry_policy or self.client._default_retry_policy
        try_count = 0
        last_exception = None
        while True:
            try:
                instances_to_skip = try_count
                return await self.request(
                    message,
                    request_field,
                    routing_type,
                    timeout,
                    acceptable_duration_ms,
                    metadata,
                    instances_to_skip,
                )
            except ServiceUnavailable as e:
                if last_exception:
                    # ran out of secondaries to try
                    raise last_exception

                if (
                    retry_policy
                    and try_count < retry_policy['max_secondaries_to_retry']
                ):
                    # Retry ServiceUnavailable (empty ring at startup) with backoff
                    try_count += 1
                    await asyncio.sleep(0.5 * try_count)
                    continue

                raise e
            except RPCException as e:
                last_exception = e
                error_code = e.code()
                if (
                    retry_policy
                    and error_code in retry_policy['retryable_grpc_status_codes']
                    and try_count < retry_policy['max_secondaries_to_retry']
                ):
                    try_count += 1
                    await asyncio.sleep(0.5 * try_count)
                    continue

                raise e

    async def request(
        self,
        message: Request,
        request_field: Optional[str] = None,
        routing_type: Optional[int] = None,
        timeout: Optional[float] = None,
        acceptable_duration_ms: Optional[int] = None,
        metadata: Optional[List[Tuple[str, str]]] = None,
        instances_to_skip: int = 0,
    ) -> Response:
        pb2_message = self._to_proto(message)
        start = time_ns()
        tags = [f'service:{self.service_name}', f'resource_name:{self.method_name}']
        grpc_code = _GRPC_CODE_FALLBACK
        acceptable_duration_ms = acceptable_duration_ms or self.client.acceptable_duration_ms

        with maybe_start_span(PIGEON_REQUEST_SPAN_NAME, self.client._peer_service, self.method_name):
            span = current_span()
            set_protocol(span)

            try:
                response = await self.client.request(
                    self.method_name,
                    pb2_message,
                    request_field=request_field,
                    routing_type=routing_type,
                    timeout=timeout,
                    metadata=metadata,
                    instances_to_skip=instances_to_skip,
                )

                grpc_code = grpc.StatusCode.OK
                duration_tag = 'classification:acceptable'

                duration_ms = round((time_ns() - start) / 1000000)
                if acceptable_duration_ms and duration_ms > acceptable_duration_ms:
                    duration_tag = 'classification:unacceptable'

                tags.append(duration_tag)

                return self._from_proto(response)
            except grpc.RpcError as e:
                error = RPCException(self.service_name, self.method_name, e)
                grpc_code = error.code()

                span.set_tag(ERROR_MSG, str(error))

                raise error
            except ServiceUnavailable as e:
                grpc_code = grpc.StatusCode.UNAVAILABLE

                span.set_tag(ERROR_MSG, str(e))

                raise e
            finally:
                http_code = _GRPC_HTTP_CODE_TRANSLATIONS[grpc_code]

                span.set_tag(GRPC_STATUS_CODE_KEY, grpc_code)
                # Setting the HTTP status code on the tag is a hack to generate more metrics from APM,
                # since Datadog automatically generates metrics based on HTTP status but not based on gRPC.
                span.set_tag(STATUS_CODE, http_code)

                if http_code >= 500:
                    span.error = 1

                # Group the codes so they're easier to use, e.g., in SLOs.
                # NOTE: At the time of implementation, tracking the actual status didn't seem super useful.
                # If you stumble upon this and feel some reason to add it, by all means do!
                if http_code >= 200 and http_code < 300:
                    tags.append('status_class:ok')
                    tags.append('http.status_class:2xx')
                elif http_code >= 400 and http_code < 500:
                    tags.append('status_class:client_error')
                    tags.append('http.status_class:4xx')
                # This deliberately captures all unexpected codes (1xx, 3xx, and anything else that might
                # somehow occur) as "5xx"/"server_error" so that they can be grouped as server errors
                # for free, e.g., for use in an SLO denominator.
                # NOTE: Based on the gRPC <> HTTP mapping, such codes should never happen.
                else:
                    tags.append('status_class:server_error')
                    tags.append('http.status_class:5xx')

                    if http_code < 500 or http_code >= 600:
                        metrics.increment(
                            'pigeon.requests.unexpected_code',
                            tags=[f'http.status_code:{http_code}', f'grpc.status_code:{grpc_code}'],
                        )

                # In most cases, the generated "HTTP" metrics mentioned above should be sufficient to track
                # the availability of a gRPC endpoint from the client's perspective - unless we also want to
                # directly account for latency, i.e., whether or not the response was fast enough.
                # We only explicitly track metrics independent of APM in these cases, as otherwise the data
                # is effectively redundant and we don't want to unnecessarily create a zillion unnecessary
                # but expensive distinct metrics.
                if acceptable_duration_ms:
                    metrics.increment('pigeon.requests', tags=tags)

    def _to_proto(self, message: Request) -> Message:
        return cast(Message, message)

    def _from_proto(self, message: Message) -> Response:
        return cast(Response, message)


# Keep the sync name as an alias for backward compatibility in type annotations
UnaryUnaryRpcCallable = AsyncUnaryUnaryRpcCallable


def set_protocol(span):
    if span:
        span.set_tag('protocol', 'grpc')


def maybe_start_span(name: str, service: str, resource: str) -> Span:
    existing_span = current_span()
    if existing_span.name == name:
        # Add tags to the existing span to indicate the inner service and resource that would have been recorded
        # if we had started a new span (otherwise we'd just lose this information).
        existing_span.set_tag('pigeon.request.subsumed', True)
        existing_span.set_tag('pigeon.request.service', service)
        existing_span.set_tag('pigeon.request.resource', resource)

        # Don't start a new span if we're already in a pigeon.<blah> span
        # This is useful for the Service RPC Guard, which cheats by using the same span name to subsume ownership of
        # the whole request, including the pigeon.request span.
        # But don't return the parent span because we don't want close it at the end of the 'with' block
        return noop_span()
    # No existing span, so start a new one
    return trace(name, service=service, resource=resource)
