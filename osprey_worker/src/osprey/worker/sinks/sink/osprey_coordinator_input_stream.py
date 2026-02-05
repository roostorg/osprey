import json
import random
import time
from collections.abc import Iterator
from queue import SimpleQueue as Queue
from typing import Any

import grpc
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
from osprey.worker.lib.utils.input_stream_ready_signaler import InputStreamReadySignaler
from osprey.worker.sinks.utils.acking_contexts import BaseAckingContext, NoopAckingContext, VerdictsAckingContext

from .input_stream import BaseInputStream

logger = get_logger()

MIN_SECONDS_BEFORE_RECONNECT = 60
SECONDS_BEFORE_RECONNECT_JITTER = 60


class GrpcConnectionDiscoveryPool:
    def __init__(self, service_name: str) -> None:
        self._service_name = service_name
        directory = Directory.instance(secure=False)

        self._grpc_channels: dict[Service, tuple[grpc.Channel, Service]] = {
            service: (GrpcConnectionDiscoveryPool._create_insecure_channel(service), service)
            for service in directory.select_all(self._service_name)
        }

        self._service_watcher = directory.get_watcher(self._service_name)
        # Adding a bound method to a WeakSet directly causes it to be removed
        # immediately, so our handler method is never invoked. Instead, create a
        # strong reference to the method on `self` and pass that to the set.
        self._handle_service_change_fn = self._handle_service_change
        self._service_watcher.add_lazy_listener(self._handle_service_change_fn)

    @classmethod
    def _create_insecure_channel(cls, service: Service) -> grpc.Channel:
        return grpc.insecure_channel(target=f'{service.connection_address}:{service.grpc_port}')

    def _handle_service_change(self, service_state: str, service: Service) -> None:
        if service_state == 'up':
            if not self._grpc_channels.get(service):
                self._grpc_channels[service] = (GrpcConnectionDiscoveryPool._create_insecure_channel(service), service)
        elif service_state == 'down':
            if self._grpc_channels.get(service):
                del self._grpc_channels[service]

    def get_connection(self) -> tuple[grpc.Channel, Service]:
        """
        Gets a GRPC connection to a service.

        If no services are registered, blocks until a service is online
        """
        channels = list(self._grpc_channels.values())

        while len(channels) == 0:
            logger.info(f'all {self._service_name} instances offline... waiting')
            time.sleep(1)
            channels = list(self._grpc_channels.values())

        return random.choice(channels)


class OspreyCoordinatorBiDirectionalStream(BaseInputStream[OspreyCoordinatorAction]):
    """
    Iterator that opens a bidirectional stream with the osprey coordinator and sends an initial action request

    Exposes methods for sending requests to the osprey coordinator

    Can potentially block forever if left without a request to send.
    """

    _STOP_STREAMING_SIGNAL = object()
    HEARTBEAT_INTERVAL_SECONDS = 30

    def __init__(self, client_id: str, channel: grpc.Channel, service: Service) -> None:
        super().__init__()
        self._client_id = client_id
        self._outgoing_request_queue: Queue[Request] = Queue()
        osprey_coordinator_stub = OspreyCoordinatorServiceStub(channel=channel)
        streaming_iterator = iter(
            self._outgoing_request_queue.get, OspreyCoordinatorBiDirectionalStream._STOP_STREAMING_SIGNAL
        )
        # timeout must be `None` or the stream reconnects every N seconds creating a new connection each time
        # but leaving the old connection open.
        self._incoming_stream = osprey_coordinator_stub.OspreyBidirectionalStream(streaming_iterator, timeout=None)
        self._tags = [f'coordinator_connection_address:{service.connection_address}']
        self._connect_time: float | None = None

    def _send(self, request: Request) -> None:
        self._outgoing_request_queue.put(request)

    def _enqueue_stop_stream_signal(self) -> None:
        # The streaming iterator is designed to stop the stream when it receives a specific python object
        # If we encoded this into the type definitions then the queue would be ok receiving any `object`
        # so we just enqueue it here as a quick hack
        self._outgoing_request_queue.put(OspreyCoordinatorBiDirectionalStream._STOP_STREAMING_SIGNAL)  # type: ignore

    def send_graceful_disconnect(self, ack_id: int, ack: bool = True, verdicts: Verdicts | None = None) -> None:
        ack_or_nack = (
            AckOrNack(ack_id=ack_id, ack=Ack(verdicts=verdicts if verdicts else None))
            if ack
            else AckOrNack(ack_id=ack_id, nack=Nack())
        )
        metrics.increment('ack_or_nack.disconnect', tags=[f'ack:{ack}', f'verdicts:{verdicts is not None}'])
        logger.debug('submitting acking disconnect')
        self._outgoing_request_queue.put(Request(disconnect=Disconnect(ack_or_nack=ack_or_nack)))
        self._enqueue_stop_stream_signal()

    def send_ack_or_nack(self, ack_id: int, ack: bool = True, verdicts: Verdicts | None = None) -> None:
        ack_or_nack = (
            AckOrNack(ack_id=ack_id, ack=Ack(verdicts=verdicts if verdicts else None))
            if ack
            else AckOrNack(ack_id=ack_id, nack=Nack())
        )
        req = Request(action_request=ActionRequest(ack_or_nack=ack_or_nack))
        metrics.increment('ack_or_nack', tags=[f'ack:{ack}', f'verdicts:{verdicts is not None}'])
        logger.debug('submitting acking action request')
        self._last_action_request_time = time.time()
        self._outgoing_request_queue.put(req)

    def get_uptime(self) -> float:
        assert self._connect_time is not None, 'This was called before a connection was established'
        return time.time() - self._connect_time

    def _gen(self) -> Iterator[OspreyCoordinatorAction]:
        logger.debug('submitting initial action request')
        self._send(Request(action_request=ActionRequest(initial=ClientDetails(id=self._client_id))))
        self._last_action_request_time = time.time()
        self._connect_time = time.time()
        metrics.increment('osprey_coordinator_input_stream.connect', tags=self._tags)

        try:
            for osprey_coordinator_action in self._incoming_stream:
                elapsed_time_since_last_action_request = time.time() - self._last_action_request_time
                metrics.histogram(
                    'osprey_coordinator_input_stream.elapsed_time_since_action_request',
                    elapsed_time_since_last_action_request,
                    tags=self._tags,
                )
                yield osprey_coordinator_action
        except grpc.RpcError as e:
            if e.code() != grpc.StatusCode.CANCELLED:
                logger.exception(e)
                sentry_sdk.capture_exception()
                logger.error('Received failure from stream...closing stream')
            metrics.increment(
                'osprey_coordinator_input_stream.stream_error',
                tags=self._tags + [f'rpc_error_code:{e.code().name.lower()}'],
            )


class OspreyCoordinatorInputStream(BaseInputStream[BaseAckingContext[OspreyEngineAction]]):
    """
    Input stream to be used by the RuleSink

    Uses `OspreyCoordinatorBiDirectionalStream` to communicate with the osprey coordinator.

    Handles:
    * Reconnecting every N seconds
    * Converting the OspreyCoordinatorActions -> OspreyEngineActions
    * Shutdown signals
    * Acking/Nacking actions
    """

    _STOP_STREAMING_SIGNAL = object()

    def __init__(
        self,
        client_id: str,
        input_stream_ready_signaler: InputStreamReadySignaler | None = None,
        coordinator_service_name: str = 'osprey_coordinator',
    ) -> None:
        super().__init__()

        self._outgoing_request_queue: Queue[Request] = Queue()
        self._soft_shutdown_signal_received = False
        self._channel_pool = GrpcConnectionDiscoveryPool(coordinator_service_name)
        self._client_id = client_id
        self._input_stream_ready_signaler = input_stream_ready_signaler
        self._current_execution_result: ExecutionResult | None = None

    def stop(self) -> None:
        if self._soft_shutdown_signal_received:
            logger.info('Received hard shutdown - exiting')
            import sys

            sys.exit()
        logger.info('Recieved shutdown signal... safely shutting down... hit ctrl-c again to hard shut down')
        self._soft_shutdown_signal_received = True

    def _create_osprey_engine_action(
        self, osprey_coordinator_action: OspreyCoordinatorAction
    ) -> OspreyEngineAction | None:
        try:
            tags = [f'action_name:{osprey_coordinator_action.action_name}']

            secret_data: dict[str, Any] = {}
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
                    from osprey.worker.adaptor.plugin_manager import bootstrap_action_proto_deserializer

                    deserializer = bootstrap_action_proto_deserializer()
                    if deserializer is not None:
                        res = deserializer.proto_bytes_to_dict(osprey_coordinator_action.proto_action_data)
                        data = res.data
                        encoding = 'proto'
                    else:
                        logger.warning('Proto deserializer plugin not available, falling back to JSON processing')
                        # Fall back to treating proto_action_data as JSON
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

    def _gen(self) -> Iterator[NoopAckingContext[OspreyEngineAction]]:
        should_run = True
        while should_run:
            channel, service = self._channel_pool.get_connection()
            bidirectional_stream = OspreyCoordinatorBiDirectionalStream(
                client_id=self._client_id, channel=channel, service=service
            )
            max_uptime_allowed = MIN_SECONDS_BEFORE_RECONNECT + random.uniform(0, SECONDS_BEFORE_RECONNECT_JITTER)
            actions_handled = 0
            for osprey_coordinator_action in bidirectional_stream:
                actions_handled += 1
                ack_id = osprey_coordinator_action.ack_id
                osprey_engine_action = self._create_osprey_engine_action(osprey_coordinator_action)
                if not osprey_engine_action:
                    info_log_osprey_action(
                        osprey_coordinator_action.action_id,
                        osprey_coordinator_action.action_name,
                        "nacking (couldn't create OspreyEngineAction)",
                    )
                    bidirectional_stream.send_ack_or_nack(ack_id, ack=False)
                    continue

                context: VerdictsAckingContext[OspreyEngineAction] = VerdictsAckingContext(osprey_engine_action)
                with metrics.timed(
                    'osprey_coordinator_input_stream.action_handle_time',
                    tags=[f'action_name:{osprey_engine_action.action_name}'],
                    use_ms=True,
                ):
                    yield context

                # First prioritize shutdown signals so we can ack the last action and disconnect gracefully
                if self._soft_shutdown_signal_received:
                    should_run = False
                    bidirectional_stream.send_graceful_disconnect(ack_id, verdicts=context.get_verdicts())
                    continue

                if (
                    self._input_stream_ready_signaler is not None
                    and self._input_stream_ready_signaler.should_pause_input_stream()
                ):
                    logger.info('Disconnecting due to input from input stream ready signaler')
                    bidirectional_stream.send_graceful_disconnect(ack_id, verdicts=context.get_verdicts())
                    self._input_stream_ready_signaler.wait_until_resume()

                # Next prioritize reconnections caused by a set amount of time having passed
                uptime = bidirectional_stream.get_uptime()
                if uptime > max_uptime_allowed:
                    logger.debug(f'Reconnecting because {uptime} seconds have passed')
                    bidirectional_stream.send_graceful_disconnect(ack_id, verdicts=context.get_verdicts())
                    continue

                # Finally if none of the previous conditions have been met we ack the last action
                bidirectional_stream.send_ack_or_nack(ack_id, verdicts=context.get_verdicts())
                info_log_osprey_action(
                    osprey_coordinator_action.action_id, osprey_coordinator_action.action_name, 'acking'
                )

            if should_run:
                metrics.gauge('osprey_coordinator_input_stream.actions_handled', actions_handled)
                logger.debug(f'Reconnecting due to stream ending, actions handled: {actions_handled}')
            else:
                logger.info('shutting down')
                metrics.increment('osprey_coordinator_input_stream.shutdown')
