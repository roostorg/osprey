import asyncio
import json
import random
import time
from typing import Any, AsyncIterator, Dict, Optional, Tuple

import grpc
import grpc.aio
import pytz
import sentry_sdk
from osprey.engine.executor.execution_context import Action as OspreyEngineAction
from osprey.engine.executor.execution_context import ExecutionResult
from osprey.rpc.common.v1.verdicts_pb2 import Verdicts
from osprey.rpc.osprey_coordinator.bidirectional_stream.v1.service_pb2 import (
    Ack,
    AckOrNack,
    ActionRequest,
    ClientDetails,
    Disconnect,
    Nack,
    OspreyCoordinatorAction,
    Request,
)
from osprey.rpc.osprey_coordinator.bidirectional_stream.v1.service_pb2_grpc import (
    OspreyCoordinatorServiceStub,
)
from osprey.worker.lib.discovery.directory import Directory
from osprey.worker.lib.discovery.service import Service
from osprey.worker.lib.instruments import metrics
from osprey.worker.lib.osprey_shared.logging import get_logger, info_log_osprey_action
from osprey.worker.sinks.utils.acking_contexts import BaseAckingContext, NoopAckingContext, VerdictsAckingContext

from osprey.async_worker.sinks.sink.input_stream import AsyncBaseInputStream

logger = get_logger()

MIN_SECONDS_BEFORE_RECONNECT = 60
SECONDS_BEFORE_RECONNECT_JITTER = 60


class AsyncVerdictsAckingContext(VerdictsAckingContext[OspreyEngineAction]):
    """Async-compatible verdicts acking context that sends ack/nack back through the bidirectional stream.

    Holds a reference to the stream and ack_id so the rules sink can use it as a normal
    context manager, and the ack is sent when the context exits.
    """

    def __init__(
        self,
        item: OspreyEngineAction,
        stream: 'OspreyCoordinatorBiDirectionalStream',
        ack_id: int,
    ) -> None:
        super().__init__(item)
        self._stream = stream
        self._ack_id = ack_id


class GrpcConnectionDiscoveryPool:
    """Maintains a pool of async gRPC channels discovered via etcd service discovery."""

    def __init__(self, service_name: str) -> None:
        self._service_name = service_name
        directory = Directory.instance(secure=False)

        self._grpc_channels: Dict[Service, Tuple[grpc.aio.Channel, Service]] = {
            service: (self._create_async_channel(service), service)
            for service in directory.select_all(self._service_name)
        }

        self._service_watcher = directory.get_watcher(self._service_name)
        # Adding a bound method to a WeakSet directly causes it to be removed
        # immediately, so our handler method is never invoked. Instead, create a
        # strong reference to the method on `self` and pass that to the set.
        self._handle_service_change_fn = self._handle_service_change
        self._service_watcher.add_lazy_listener(self._handle_service_change_fn)

    @staticmethod
    def _create_async_channel(service: Service) -> grpc.aio.Channel:
        return grpc.aio.insecure_channel(target=f'{service.connection_address}:{service.grpc_port}')

    def _handle_service_change(self, service_state: str, service: Service) -> None:
        if service_state == 'up':
            if not self._grpc_channels.get(service):
                self._grpc_channels[service] = (self._create_async_channel(service), service)
        elif service_state == 'down':
            if self._grpc_channels.get(service):
                del self._grpc_channels[service]

    async def get_connection(self) -> Tuple[grpc.aio.Channel, Service]:
        """Gets an async gRPC channel to a coordinator instance.

        If no services are registered, polls until one becomes available.
        """
        channels = list(self._grpc_channels.values())

        while len(channels) == 0:
            logger.info(f'all {self._service_name} instances offline... waiting')
            await asyncio.sleep(1)
            channels = list(self._grpc_channels.values())

        return random.choice(channels)


class OspreyCoordinatorBiDirectionalStream:
    """Manages a single bidirectional gRPC stream with the osprey coordinator.

    Outgoing requests (initial ClientDetails, then ack/nack) are fed through an
    asyncio.Queue and yielded as an async iterator to the gRPC call. Incoming
    OspreyCoordinatorAction messages are exposed via ``async for``.
    """

    HEARTBEAT_INTERVAL_SECONDS = 30

    def __init__(self, client_id: str, channel: grpc.aio.Channel, service: Service) -> None:
        self._client_id = client_id
        self._outgoing_queue: asyncio.Queue[Optional[Request]] = asyncio.Queue()
        self._stub = OspreyCoordinatorServiceStub(channel=channel)
        self._tags = [f'coordinator_connection_address:{service.connection_address}']
        self._connect_time: Optional[float] = None
        self._last_action_request_time: float = 0.0
        self._stopped = False

    # -- outgoing request helpers --------------------------------------------------

    async def _outgoing_iterator(self) -> AsyncIterator[Request]:
        """Async generator that drains the outgoing queue for the gRPC call."""
        while True:
            request = await self._outgoing_queue.get()
            if request is None:
                # Sentinel value — stop the outgoing side of the stream
                return
            yield request

    async def _send(self, request: Request) -> None:
        await self._outgoing_queue.put(request)

    async def _enqueue_stop_signal(self) -> None:
        await self._outgoing_queue.put(None)

    async def send_graceful_disconnect(
        self, ack_id: int, ack: bool = True, verdicts: Optional[Verdicts] = None
    ) -> None:
        ack_or_nack = (
            AckOrNack(ack_id=ack_id, ack=Ack(verdicts=verdicts if verdicts else None))
            if ack
            else AckOrNack(ack_id=ack_id, nack=Nack())
        )
        metrics.increment('ack_or_nack.disconnect', tags=[f'ack:{ack}', f'verdicts:{verdicts is not None}'])
        logger.debug('submitting acking disconnect')
        await self._outgoing_queue.put(Request(disconnect=Disconnect(ack_or_nack=ack_or_nack)))
        await self._enqueue_stop_signal()

    async def send_ack_or_nack(
        self, ack_id: int, ack: bool = True, verdicts: Optional[Verdicts] = None
    ) -> None:
        ack_or_nack = (
            AckOrNack(ack_id=ack_id, ack=Ack(verdicts=verdicts if verdicts else None))
            if ack
            else AckOrNack(ack_id=ack_id, nack=Nack())
        )
        req = Request(action_request=ActionRequest(ack_or_nack=ack_or_nack))
        metrics.increment('ack_or_nack', tags=[f'ack:{ack}', f'verdicts:{verdicts is not None}'])
        logger.debug('submitting acking action request')
        self._last_action_request_time = time.time()
        await self._outgoing_queue.put(req)

    def get_uptime(self) -> float:
        assert self._connect_time is not None, 'This was called before a connection was established'
        return time.time() - self._connect_time

    # -- incoming action iteration -------------------------------------------------

    async def __aiter__(self) -> AsyncIterator[OspreyCoordinatorAction]:
        async for action in self._gen():
            yield action

    async def _gen(self) -> AsyncIterator[OspreyCoordinatorAction]:
        logger.debug('submitting initial action request')
        await self._send(Request(action_request=ActionRequest(initial=ClientDetails(id=self._client_id))))
        self._last_action_request_time = time.time()
        self._connect_time = time.time()
        metrics.increment('osprey_coordinator_input_stream.connect', tags=self._tags)

        try:
            incoming_stream = self._stub.OspreyBidirectionalStream(self._outgoing_iterator(), timeout=None)
            async for osprey_coordinator_action in incoming_stream:
                elapsed_time_since_last_action_request = time.time() - self._last_action_request_time
                metrics.histogram(
                    'osprey_coordinator_input_stream.elapsed_time_since_action_request',
                    elapsed_time_since_last_action_request,
                    tags=self._tags,
                )
                yield osprey_coordinator_action
        except grpc.aio.AioRpcError as e:
            if e.code() != grpc.StatusCode.CANCELLED:
                logger.exception(e)
                sentry_sdk.capture_exception()
                logger.error('Received failure from stream...closing stream')
            metrics.increment(
                'osprey_coordinator_input_stream.stream_error',
                tags=self._tags + [f'rpc_error_code:{e.code().name.lower()}'],
            )


class OspreyCoordinatorInputStream(AsyncBaseInputStream[BaseAckingContext[OspreyEngineAction]]):
    """Async input stream for the coordinator bidirectional gRPC transport.

    Wraps ``OspreyCoordinatorBiDirectionalStream`` and handles:
    * Reconnecting on a jittered interval
    * Deserializing OspreyCoordinatorAction -> OspreyEngineAction
    * Graceful shutdown via ``asyncio.Event``
    * Acking / nacking actions back through the stream
    """

    def __init__(
        self,
        client_id: str,
        coordinator_service_name: str = 'osprey_coordinator',
    ) -> None:
        self._client_id = client_id
        self._channel_pool = GrpcConnectionDiscoveryPool(coordinator_service_name)
        self._shutdown_event = asyncio.Event()
        self._current_execution_result: Optional[ExecutionResult] = None

    @classmethod
    def from_direct_address(cls, client_id: str, address: str, service_name: str = 'osprey_coordinator') -> 'OspreyCoordinatorInputStream':
        """Create an input stream connected directly to a coordinator address.

        Bypasses etcd service discovery, which uses gevent-patched code that
        is incompatible with grpc.aio. Use this for the async worker.
        """
        instance = cls.__new__(cls)
        instance._client_id = client_id
        instance._shutdown_event = asyncio.Event()
        instance._current_execution_result = None

        host, port_str = address.rsplit(':', 1)
        service = Service(
            name=service_name,
            address=host,
            port=int(port_str),
            ports={'grpc': int(port_str)},
            metadata={},
        )
        channel = grpc.aio.insecure_channel(address)

        pool = GrpcConnectionDiscoveryPool.__new__(GrpcConnectionDiscoveryPool)
        pool._service_name = service_name
        pool._grpc_channels = {service: (channel, service)}
        pool._service_watcher = None
        instance._channel_pool = pool

        return instance

    async def stop(self) -> None:
        logger.info('Received shutdown signal... safely shutting down')
        self._shutdown_event.set()

    # -- deserialization -----------------------------------------------------------

    def _create_osprey_engine_action(
        self, osprey_coordinator_action: OspreyCoordinatorAction
    ) -> Optional[OspreyEngineAction]:
        try:
            tags = [f'action_name:{osprey_coordinator_action.action_name}']

            secret_data: Dict[str, Any] = {}
            which_of_action_data = osprey_coordinator_action.WhichOneof('action_data')
            if which_of_action_data == 'json_action_data':
                info_log_osprey_action(
                    osprey_coordinator_action.action_id,
                    osprey_coordinator_action.action_name,
                    'received json-encoded action',
                )
                with metrics.timed(
                    'osprey_coordinator_input_stream.deserialize_message',
                    tags=tags + ['serialization_type:json'],
                    use_ms=True,
                ):
                    data = json.loads(osprey_coordinator_action.json_action_data)
                    if osprey_coordinator_action.HasField('json_secret_data'):
                        secret_data = json.loads(osprey_coordinator_action.json_secret_data)
                encoding = 'json'

            elif which_of_action_data == 'proto_action_data':
                info_log_osprey_action(
                    osprey_coordinator_action.action_id,
                    osprey_coordinator_action.action_name,
                    'received proto-encoded action',
                )
                with metrics.timed(
                    'osprey_coordinator_input_stream.deserialize_message',
                    tags=tags + ['serialization_type:proto'],
                    use_ms=True,
                ):
                    from osprey.async_worker.adaptor.plugin_manager import bootstrap_async_action_proto_deserializer

                    deserializer = bootstrap_async_action_proto_deserializer()
                    if deserializer is not None:
                        res = deserializer.proto_bytes_to_dict(osprey_coordinator_action.proto_action_data)
                        data = res.data
                        encoding = 'proto'
                    else:
                        logger.warning('Proto deserializer plugin not available, falling back to JSON processing')
                        data = json.loads(osprey_coordinator_action.proto_action_data)
                        encoding = 'json'
            else:
                metrics.increment(
                    'osprey_coordinator_input_stream.deserialize_message_failure',
                    tags=tags + ['failure:invalid_serialization_type'],
                )
                return None

            assert isinstance(data, dict), 'the `json_action_data` was not a dict'
            assert isinstance(secret_data, dict), 'the `json_secret_data` was not a dict'
            assert osprey_coordinator_action.action_name != '', 'action name must never be empty'

            return OspreyEngineAction(
                action_id=osprey_coordinator_action.action_id,
                action_name=osprey_coordinator_action.action_name,
                data=data,
                secret_data=secret_data,
                timestamp=osprey_coordinator_action.timestamp.ToDatetime(tzinfo=pytz.utc),
                encoding=encoding,
            )
        except Exception:
            logger.exception('Error while generating input message')
            sentry_sdk.capture_exception()
            metrics.increment(
                'osprey_coordinator_input_stream.deserialize_message_failure',
                tags=tags + ['failure:unknown_exc'],
            )
            return None

    # -- main loop -----------------------------------------------------------------

    async def _gen(self) -> AsyncIterator[BaseAckingContext[OspreyEngineAction]]:
        while not self._shutdown_event.is_set():
            channel, service = await self._channel_pool.get_connection()
            bidirectional_stream = OspreyCoordinatorBiDirectionalStream(
                client_id=self._client_id, channel=channel, service=service
            )
            max_uptime_allowed = MIN_SECONDS_BEFORE_RECONNECT + random.uniform(0, SECONDS_BEFORE_RECONNECT_JITTER)
            actions_handled = 0

            async for osprey_coordinator_action in bidirectional_stream:
                actions_handled += 1
                ack_id = osprey_coordinator_action.ack_id
                osprey_engine_action = self._create_osprey_engine_action(osprey_coordinator_action)

                if not osprey_engine_action:
                    info_log_osprey_action(
                        osprey_coordinator_action.action_id,
                        osprey_coordinator_action.action_name,
                        "nacking (couldn't create OspreyEngineAction)",
                    )
                    await bidirectional_stream.send_ack_or_nack(ack_id, ack=False)
                    continue

                context: AsyncVerdictsAckingContext = AsyncVerdictsAckingContext(
                    osprey_engine_action, bidirectional_stream, ack_id
                )
                with metrics.timed(
                    'osprey_coordinator_input_stream.action_handle_time',
                    tags=[f'action_name:{osprey_engine_action.action_name}'],
                    use_ms=True,
                ):
                    yield context

                # Prioritize shutdown so we can ack the last action and disconnect gracefully
                if self._shutdown_event.is_set():
                    await bidirectional_stream.send_graceful_disconnect(ack_id, verdicts=context.get_verdicts())
                    break

                # Reconnect after the jittered uptime threshold
                uptime = bidirectional_stream.get_uptime()
                if uptime > max_uptime_allowed:
                    logger.debug(f'Reconnecting because {uptime} seconds have passed')
                    await bidirectional_stream.send_graceful_disconnect(ack_id, verdicts=context.get_verdicts())
                    break

                # Normal path: ack the last action and request the next one
                await bidirectional_stream.send_ack_or_nack(ack_id, verdicts=context.get_verdicts())
                info_log_osprey_action(
                    osprey_coordinator_action.action_id, osprey_coordinator_action.action_name, 'acking'
                )

            if not self._shutdown_event.is_set():
                metrics.gauge('osprey_coordinator_input_stream.actions_handled', actions_handled)
                logger.debug(f'Reconnecting due to stream ending, actions handled: {actions_handled}')
            else:
                logger.info('shutting down')
                metrics.increment('osprey_coordinator_input_stream.shutdown')
