import abc
from datetime import datetime
from types import TracebackType
from typing import Dict, Generic, List, Optional, Type, TypeVar, Union

import gevent
from google.api_core.exceptions import DeadlineExceeded
from google.cloud.pubsub_v1 import SubscriberClient
from google.cloud.pubsub_v1.subscriber.message import Message
from osprey.rpc.common.v1.verdicts_pb2 import Verdicts
from osprey.worker.lib.instruments import metrics
from osprey.worker.lib.osprey_shared.logging import get_logger

logger = get_logger()

_T = TypeVar('_T')


# TODO: support NACK
class BaseAckingContext(abc.ABC, Generic[_T]):
    """An acking context for handling single actions from input streams."""

    def __init__(self, item: _T) -> None:
        super().__init__()
        self._item: _T = item
        self._should_nack = False
        self._publish_time = datetime.now()
        self._attributes: Optional[Dict[str, str]] = None

    @abc.abstractmethod
    def _ack(self) -> None:
        """Acknowledges the message or item that this Acking Context holds."""

        raise NotImplementedError

    @abc.abstractmethod
    def _nack(self) -> None:
        """NACKs the message or item that this Acking Context holds."""

        raise NotImplementedError

    @property
    def attributes(self) -> Optional[Dict[str, str]]:
        return self._attributes

    def mark_as_nack(self) -> None:
        self._should_nack = True

    def __enter__(self) -> _T:
        return self._item

    def __exit__(
        self,
        exc_type: Union[Type[BaseException], None],
        exc_value: Union[BaseException, None],
        exc_traceback: Union[TracebackType, None],
    ) -> None:
        if self._should_nack:
            self._nack()
        else:
            self._ack()

    @property
    def publish_time(self) -> datetime:
        return self._publish_time


class NoopAckingContext(BaseAckingContext[_T]):
    """A context manager for handling single actions require no acking operations from input streams."""

    def _ack(self) -> None:
        return

    def _nack(self) -> None:
        return


class VerdictsAckingContext(NoopAckingContext[_T]):
    """
    A context manager for storing verdicts from the rules sink inside of a NoopAckingContext :3

    This is used to send verdicts back to the Osprey Coordinator, if any were captured~
    """

    def __init__(self, item: _T) -> None:
        super().__init__(item)
        self._verdicts: Optional[Verdicts] = None

    def set_verdicts(self, verdicts: Verdicts) -> None:
        self._verdicts = verdicts

    def get_verdicts(self) -> Optional[Verdicts]:
        return self._verdicts


class PubSubMessageAckingContext(BaseAckingContext[_T]):
    """A context manager for handling single pubsub messages using the push method.
    Ennsures that the handling and acking of a specific message will be handled by the same thread."""

    def __init__(self, item: _T, message: Message):
        super().__init__(item)
        self._message = message

    def _ack(self) -> None:
        self._message.ack()

    def _nack(self) -> None:
        self._message.nack()


class PullPubSubMessageContext(BaseAckingContext[_T]):
    """A context manager for handling pubsub messages using the pull method.
    Ensures that the handling and acking of a specific message will be handled by the same thread."""

    def __init__(
        self,
        item: _T,
        subscriber: SubscriberClient,
        subscription_path: str,
        ack_ids: List[str],
        publish_time: Optional[datetime] = None,
        attributes: Optional[Dict[str, str]] = None,
    ):
        super().__init__(item)
        self._subscriber = subscriber
        self._subscription_path = subscription_path
        self._original_ack_ids = ack_ids
        # True to ACK, False to NACK. Defaults to ACK all.
        self._ack_statuses: Dict[str, bool] = {ack_id: True for ack_id in ack_ids}
        self._timeout = 1.5
        self._publish_time = publish_time if publish_time else datetime.now()
        self._attributes = attributes

    @property
    def original_ack_ids(self) -> List[str]:
        return self._original_ack_ids

    def mark_ack_id_for_nack(self, ack_id_to_nack: str) -> None:
        if ack_id_to_nack in self._ack_statuses:
            self._ack_statuses[ack_id_to_nack] = False

    def mark_all_for_nack(self) -> None:
        for ack_id in self._ack_statuses:
            self._ack_statuses[ack_id] = False

    def __exit__(
        self,
        exc_type: Union[Type[BaseException], None],
        exc_value: Union[BaseException, None],
        exc_traceback: Union[TracebackType, None],
    ) -> None:
        # if exc_type is not None:
        #     logger.error(f'Exception in PullPubSubMessageContext, NACKing all messages: {exc_value}')
        #     self.mark_all_for_nack()

        should_ack = any(should_ack for should_ack in self._ack_statuses.values())
        should_nack = any(not should_ack for should_ack in self._ack_statuses.values())

        if should_ack:
            self._ack()
        if should_nack:
            self._nack()

    def _ack(self) -> None:
        ack_ids = [ack_id for ack_id, should_ack in self._ack_statuses.items() if should_ack]
        if not ack_ids:
            logger.debug('No ack_ids to ACK.')
            return

        try:
            # Sometimes the subscriber timeout doesn't work, so we rely on gevent.Timeout as well.
            with gevent.Timeout(self._timeout + 0.5):
                with metrics.timed(
                    'pubsub_consumer.acknowledge.duration', tags=[f'subscription_path:{self._subscription_path}']
                ):
                    self._subscriber.acknowledge(
                        subscription=self._subscription_path, ack_ids=ack_ids, timeout=self._timeout
                    )
            metrics.increment(
                'pubsub_consumer.acknowledge.success',
                value=len(ack_ids),
                tags=[f'subscription_path:{self._subscription_path}'],
            )
        except (DeadlineExceeded, gevent.Timeout):
            # Log and track metric, message will be redelivered
            logger.exception(f'Subscriber acknowledge timed out for {len(ack_ids)} messages.')
            metrics.increment(
                'pubsub_consumer.acknowledge.timeout',
                value=len(ack_ids),
                tags=[f'subscription_path:{self._subscription_path}'],
            )
        except Exception as e:
            logger.exception(f'Error during subscriber acknowledge for {len(ack_ids)} messages: {e}')
            metrics.increment(
                'pubsub_consumer.acknowledge.failure',
                value=len(ack_ids),
                tags=[f'subscription_path:{self._subscription_path}', f'error:{e.__class__.__name__}'],
            )

    def _nack(self) -> None:
        ack_ids_to_nack = [ack_id for ack_id, should_ack in self._ack_statuses.items() if not should_ack]
        if not ack_ids_to_nack:
            logger.debug('No ack_ids to NACK.')
            return

        try:
            # Sometimes the subscriber timeout doesn't work, so we rely on gevent.Timeout as well.
            with gevent.Timeout(self._timeout + 0.5):
                with metrics.timed(
                    'pubsub_consumer.nack.duration', tags=[f'subscription_path:{self._subscription_path}']
                ):
                    self._subscriber.modify_ack_deadline(
                        subscription=self._subscription_path,
                        ack_ids=ack_ids_to_nack,
                        ack_deadline_seconds=0,
                        timeout=self._timeout,
                    )
            metrics.increment(
                'pubsub_consumer.nack.success',
                value=len(ack_ids_to_nack),
                tags=[f'subscription_path:{self._subscription_path}'],
            )
        except (DeadlineExceeded, gevent.Timeout):
            logger.exception(f'Subscriber nack timed out for {len(ack_ids_to_nack)} messages.')
            metrics.increment(
                'pubsub_consumer.nack.timeout',
                value=len(ack_ids_to_nack),
                tags=[f'subscription_path:{self._subscription_path}'],
            )
        except Exception as e:
            logger.exception(f'Error during subscriber nack for {len(ack_ids_to_nack)} messages: {e}')
            metrics.increment(
                'pubsub_consumer.nack.failure',
                value=len(ack_ids_to_nack),
                tags=[f'subscription_path:{self._subscription_path}', f'error:{e.__class__.__name__}'],
            )
