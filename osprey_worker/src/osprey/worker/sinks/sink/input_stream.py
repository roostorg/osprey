import abc
import inspect
import json
from collections import deque
from collections.abc import Iterator, Sequence
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Generic, Type, TypeVar, cast

import gevent
import msgpack
import sentry_sdk
from gevent.lock import RLock
from gevent.queue import Queue as GeventQueue
from google.api_core import retry
from google.cloud.pubsub_v1 import SubscriberClient, types
from google.cloud.pubsub_v1.subscriber.message import Message
from google.protobuf.message import DecodeError
from google.protobuf.message import Message as ProtoMessage
from google.pubsub_v1 import PubsubMessage
from kafka.consumer.fetcher import ConsumerRecord
from osprey.engine.executor.execution_context import Action
from osprey.worker.lib.encryption.envelope import Envelope
from osprey.worker.lib.instruments import metrics
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.lib.storage.postgres import Model, scoped_session
from osprey.worker.lib.utils.dates import parse_go_timestamp
from osprey.worker.sinks.utils.acking_contexts import (
    BaseAckingContext,
    NoopAckingContext,
    PubSubMessageAckingContext,
    PullPubSubMessageContext,
)
from osprey.worker.sinks.utils.kafka import PatchedKafkaConsumer
from pydantic import BaseModel
from tenacity import RetryCallState, retry_if_exception_type, stop_never, wait_exponential
from tenacity import retry as tenacity_retry
from typing_extensions import Protocol

logger = get_logger()

_T = TypeVar('_T', covariant=True)
_PydanticModelT = TypeVar('_PydanticModelT', bound=BaseModel, covariant=True)


if TYPE_CHECKING:
    from google.cloud.pubsub_v1.types import PullResponse


class BaseInputStream(abc.ABC, Generic[_T]):
    """The base input stream produces objects that the sink will process."""

    def __init__(self) -> None:
        super().__init__()
        self._iterator: Iterator[_T] | None = None
        self._iterator_lock = RLock()

    @abc.abstractmethod
    def _gen(self) -> Iterator[_T]:
        """Returns an iterator which yields the objects to process.

        Will only be called once per instance of the stream, and that iterator will be re-used."""
        raise NotImplementedError

    def __iter__(self) -> Iterator[_T]:
        return self

    def __next__(self) -> _T:
        iterator = self._iterator
        if iterator is None:
            with self._iterator_lock:
                # Re-check that we haven't set it
                iterator = self._iterator
                if iterator is None:
                    iterator = self._iterator = self._gen()
        if inspect.isgenerator(iterator):
            # Generators can only have one caller at a time
            with self._iterator_lock:
                # mypy isn't able to work out what this type is.
                return cast(_T, next(iterator))

        # If not a generator, no need to wait for a lock
        return next(iterator)


class StaticInputStream(BaseInputStream[_T]):
    """An input stream that will return a static list of actions, until exhausted."""

    def __init__(self, actions: Sequence[_T]):
        super().__init__()
        self._actions = deque(actions)

    def _gen(self) -> Iterator[_T]:
        return iter(self._actions)


class StaticMessagesInputStream(
    BaseInputStream[NoopAckingContext[Action] | PullPubSubMessageContext[list[_PydanticModelT]]]
):
    """An input stream that will return a static list of contexts, until exhausted.

    This should only be used in tests to mock a stream of message contexts.
    """

    def __init__(self, contexts: Sequence[NoopAckingContext[Action] | PullPubSubMessageContext[list[_PydanticModelT]]]):
        super().__init__()
        self._contexts = list(contexts)

    def _gen(self) -> Iterator[NoopAckingContext[Action] | PullPubSubMessageContext[list[_PydanticModelT]]]:
        return iter(self._contexts)


class InfiniteStaticInputStream(BaseInputStream[_T]):
    def __init__(self, item: _T):
        super().__init__()
        self._item = item

    def _gen(self) -> Iterator[_T]:
        while True:
            yield self._item


class BasePubSubInputStream(BaseInputStream[_T]):
    def __init__(self, subscriber: SubscriberClient, subscription_path: str, max_messages: int = 250):
        super().__init__()
        self.subscriber = subscriber
        self.subscription_path = subscription_path
        self.max_messages = max_messages

    def _pull(self, max_messages: int | None = None) -> 'PullResponse':
        max_messages_to_pull = max_messages if max_messages is not None else self.max_messages
        with metrics.timed('pubsub_consumer.poll_time', tags=[f'subscription_path:{self.subscription_path}']):
            return self.subscriber.pull(
                subscription=self.subscription_path,
                max_messages=max_messages_to_pull,
                retry=retry.Retry(timeout=300),
            )


class PubSubOspreyActionInputStream(BasePubSubInputStream[BaseAckingContext[Action]]):
    def __init__(
        self,
        subscriber: SubscriberClient,
        subscription_path: str,
        kek_uri: str = '',
        max_messages: int = 250,
        gevent_queue_size: int = 1000,
    ):
        super().__init__(subscriber, subscription_path, max_messages)
        self.queue = GeventQueue(maxsize=gevent_queue_size)
        self.encryption_envelope = Envelope(kek_uri=kek_uri, gcp_credential_path='')  # use default gcp credentials

    def _handle_message_data_if_is_secure(self, message: Message) -> bytes:
        is_secure = message.attributes.get('encrypted', 'false') == 'true'
        message_data: bytes = message.data
        if is_secure:
            # if the message is encrypted then decrypt the message data before proceeding
            with metrics.timed('osprey_actions.encryption.decrypt'):
                message_data = self.encryption_envelope.decrypt(message_data)
        return message_data

    def _create_action(self, message: Message) -> Action:
        """
        Construct an Action from a pubsub Message

        Until we have strict protobuf types, if this method is changed, make sure
        content_cop/pydantic_models/messages.py::OspreyRulesInput is still compatible.
        """
        encoding = message.attributes.get('encoding')

        message_data: bytes = self._handle_message_data_if_is_secure(message)

        if encoding == 'proto':
            from osprey.worker.adaptor.plugin_manager import bootstrap_action_proto_deserializer

            deserializer = bootstrap_action_proto_deserializer()
            if deserializer is not None:
                res = deserializer.proto_bytes_to_dict(message_data)
                return Action(
                    action_id=res.action_id,
                    action_name=res.action_name,
                    data=res.data,
                    timestamp=message.publish_time,
                )
            else:
                logger.warning('Proto deserializer plugin not available, falling back to JSON processing')
        # Process as JSON (either explicitly requested or fallback from proto)
        # Right now there are two formats for loading messages
        # This code will prevent errors until the old format is flushed out
        # This will mainly affect the hasher
        try:
            # old format
            action = msgpack.loads(message_data)
            return Action(
                action_id=int(action['id']),
                action_name=action['name'],
                data=action['data'],
                secret_data=action.get('secret_data', {}),
                timestamp=message.publish_time,
            )
        except Exception:
            # new format
            action = json.loads(msgpack.loads(message_data))
            return Action(
                action_id=int(action['id']),
                action_name=action['name'],
                data=action['data'],
                secret_data=action.get('secret_data', {}),
                timestamp=message.publish_time,
            )


# TODO: this maybe not needed anymore with Coordinator
class AsyncPubSubOspreyActionInputStream(PubSubOspreyActionInputStream):
    def _worker(self) -> None:
        logger.info('Pubsub Consumer Worker spawned')

        def stream_callback(message: Message) -> None:
            with metrics.timed('pubsub_input_stream.queue.put_time'):
                self.queue.put(message)

        with self.subscriber as subscriber:
            flow_control = types.FlowControl(
                max_messages=self.max_messages,
                # assume 4KB per message
                max_bytes=4_000 * self.max_messages,
                max_lease_duration=60 * 60,
                max_duration_per_lease_extension=600,
            )
            streaming_pull_future = subscriber.subscribe(
                self.subscription_path, callback=stream_callback, flow_control=flow_control
            )

            while True:
                try:
                    streaming_pull_future.result()
                except Exception as e:
                    logger.error(e)
                    sentry_sdk.capture_exception(error=e)
                    continue

    def _gen(self) -> Iterator[PubSubMessageAckingContext[Action]]:
        gevent.spawn(self._worker)
        while True:
            try:
                with metrics.timed('pubsub_input_stream.queue.get_time'):
                    received_message = self.queue.get()
                    assert isinstance(received_message, Message)

                action = self._create_action(received_message)
                delay = datetime.now(timezone.utc) - action.timestamp
                metrics.timing('input_stream_delay', delay.total_seconds(), ['stream:pubsub_input_stream'])
                yield PubSubMessageAckingContext(action, received_message)
            except Exception:
                logger.exception('Error while generating input message')
                sentry_sdk.capture_exception()
                continue


class SynchronousPubSubMultiProtoInputStream(BasePubSubInputStream[PullPubSubMessageContext[ProtoMessage]]):
    def __init__(
        self,
        subscriber: SubscriberClient,
        subscription_path: str,
        proto_message_classes: list[Type[ProtoMessage]],
        max_messages: int = 250,
    ):
        super().__init__(subscriber, subscription_path, max_messages)
        self.proto_message_classes = proto_message_classes

    def _gen(self) -> Iterator[PullPubSubMessageContext[ProtoMessage]]:
        with SubscriberClient() as subscriber:
            while True:
                response = self._pull()
                if not response.received_messages:
                    logger.debug(f'No messages received for subscription {self.subscription_path}')
                    continue
                for received_message in response.received_messages:
                    message = received_message.message
                    assert isinstance(message, PubsubMessage)

                    item = None
                    for proto_message_class in self.proto_message_classes:
                        try:
                            item = proto_message_class.FromString(message.data)
                            break
                        except DecodeError:
                            continue

                    if not item:
                        raise DecodeError(f'Error decoding message as any of {self.proto_message_classes}')

                    yield PullPubSubMessageContext(
                        item=item,
                        subscriber=subscriber,
                        subscription_path=self.subscription_path,
                        ack_ids=[received_message.ack_id],
                        publish_time=message.publish_time.ToDatetime(),
                        attributes={k: v for k, v in message.attributes.items()},
                    )


# Use for utility scripts, not for production
class RawPubSubInputStream(BasePubSubInputStream[PullPubSubMessageContext[PubsubMessage]]):
    def _gen(self) -> Iterator[PullPubSubMessageContext[PubsubMessage]]:
        with SubscriberClient() as subscriber:
            while True:
                response = self._pull()
                if not response.received_messages:
                    continue

                for received_message in response.received_messages:
                    message = received_message.message
                    assert isinstance(message, PubsubMessage)
                    yield PullPubSubMessageContext(
                        item=message,
                        subscriber=subscriber,
                        subscription_path=self.subscription_path,
                        ack_ids=[received_message.ack_id],
                    )


class PubSubBulkInputStream(BasePubSubInputStream[PullPubSubMessageContext[list[_PydanticModelT]]]):
    def __init__(
        self, subscriber: SubscriberClient, subscription_path: str, model: Type[_PydanticModelT], bulk_size: int
    ):
        super().__init__(subscriber, subscription_path)
        self._model = model
        self._bulk_size = bulk_size

    def _gen(self) -> Iterator[PullPubSubMessageContext[list[_PydanticModelT]]]:
        with self.subscriber as subscriber:
            while True:
                try:
                    response = self._pull(max_messages=self._bulk_size)
                    if not response.received_messages:
                        logger.debug(f'No messages received for subscription {self.subscription_path}')
                        continue

                    messages: list[_PydanticModelT] = []
                    ack_ids: list[str] = []

                    for received_message in response.received_messages:
                        message = received_message.message
                        assert isinstance(message, PubsubMessage)
                        messages.append(self._create_element(message))
                        ack_ids.append(received_message.ack_id)

                    if messages:  # Only yield if we have messages to process
                        yield PullPubSubMessageContext(
                            item=messages,
                            subscriber=subscriber,
                            subscription_path=self.subscription_path,
                            ack_ids=ack_ids,
                            attributes={k: v for k, v in message.attributes.items()},
                        )
                except BaseException as e:
                    logger.exception(f'Failure in PubSubBulkInputStream._gen: {e}')
                    sentry_sdk.capture_exception()

    def _create_element(self, message: 'PubsubMessage') -> _PydanticModelT:
        return self._model.parse_raw(message.data)


_ModelT = TypeVar('_ModelT', bound=Model, covariant=True)


class _ClaimableModel(Protocol[_ModelT]):
    @classmethod
    def claim(cls) -> _ModelT | None: ...


class PostgresInputStream(BaseInputStream[_ModelT]):
    def __init__(self, model: Type[_ClaimableModel[_ModelT]], cooldown: float = 3, tags: Sequence[str] = ()):
        super().__init__()
        self._model = model
        self._cooldown = cooldown
        self._tags = tags

    def _gen(self) -> Iterator[_ModelT]:
        def _before_sleep(retry_state: RetryCallState) -> None:
            if retry_state.next_action:
                gevent.sleep(retry_state.next_action.sleep)

        def _log_before_attempt(retry_state: RetryCallState) -> None:
            if retry_state.outcome and retry_state.outcome.failed:
                exception = retry_state.outcome.exception()
                if exception:
                    logger.error(f'Failed to claim or process row: {exception}')
                    sentry_sdk.capture_exception(exception)

        @tenacity_retry(
            wait=wait_exponential(multiplier=self._cooldown, min=self._cooldown, max=self._cooldown * 10),
            stop=stop_never,
            retry=retry_if_exception_type(Exception),
            reraise=True,
            before_sleep=_before_sleep,
            before=_log_before_attempt,
        )
        def claim_with_retry() -> _ModelT | None:
            with metrics.timed('claim_postgres_row', tags=self._tags):
                return self._model.claim()

        while True:
            with scoped_session():
                data = claim_with_retry()
                if data:
                    yield data
                else:
                    gevent.sleep(self._cooldown)


class KafkaInputStream(BaseInputStream[BaseAckingContext[Action]]):
    """An input stream that consumes messages from a Kafka topic and yields Action objects wrapped in an AckingContext."""

    def __init__(self, kafka_consumer: PatchedKafkaConsumer):
        super().__init__()
        self._consumer: PatchedKafkaConsumer = kafka_consumer

    def _gen(self) -> Iterator[BaseAckingContext[Action]]:
        while True:
            try:
                with metrics.timed('kafka_consumer.lock_time'):
                    with metrics.timed('kafka_consumer.poll_time'):
                        record: ConsumerRecord = next(self._consumer)
                data = json.loads(record.value)
                timestamp = parse_go_timestamp(data['send_time'])
                action_data = data['data']
                # this was here for when this was protobuf. If its json by default, we should just assume its all in one
                # json blob.
                # action_data = json.loads(action_data_json)

                action = Action(
                    action_id=int(action_data['action_id']),
                    action_name=action_data['action_name'],
                    data=action_data['data'],
                    timestamp=timestamp,
                )
                # Wrap in NoopAckingContext for now, or implement a KafkaAckingContext if needed
                yield NoopAckingContext(action)
            except Exception as e:
                logger.exception('Error while consuming from Kafka')
                sentry_sdk.capture_exception(e)
                continue
