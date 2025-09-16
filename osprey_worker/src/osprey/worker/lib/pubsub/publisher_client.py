import abc
from concurrent import futures
from threading import Lock
from typing import Set, Union

from google.cloud import pubsub_v1
from osprey.worker.lib.instruments import metrics


class BasePubsubPublisherClient(abc.ABC):
    def __init__(self):
        pass

    @abc.abstractmethod
    def publish(self, topic: str, data: bytes) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def stop(self) -> None:
        raise NotImplementedError


class BatchPubsubPublisherClient(BasePubsubPublisherClient):
    _futures_buffer_lock: Lock = Lock()
    _futures_buffer: Set[pubsub_v1.publisher.futures.Future] = set()
    _publisher: pubsub_v1.PublisherClient

    def __init__(self, batch_settings: pubsub_v1.types.BatchSettings):
        self._publisher = pubsub_v1.PublisherClient(batch_settings=batch_settings)
        self._max_future_buffer_size = batch_settings.max_messages

    def _add_to_buffer(self, future) -> None:
        with self._futures_buffer_lock:
            self._futures_buffer.add(future)

    def _remove_from_buffer(self, future) -> None:
        with self._futures_buffer_lock:
            self._futures_buffer.remove(future)

    def publish(self, topic: str, data: bytes, **attributes: Union[bytes, str]) -> None:
        if len(self._futures_buffer) >= self._max_future_buffer_size:
            # https://github.com/googleapis/google-cloud-python/issues/6201 ????
            futures.wait(self._futures_buffer, return_when=futures.ALL_COMPLETED)
        future = self._publisher.publish(topic, data, **attributes)
        self._futures_buffer.add(future)
        future.add_done_callback(self._remove_from_buffer)

    # if we're batching (batch_size >1) messages are not guaranteed to be flushed
    # when program exists, make sure to call this method.
    def stop(self) -> None:
        # temp metrics to see if we're catching messages during flush
        metrics.increment(metric='batch_pubsub_publisher.stop.count')
        metrics.increment(metric='batch_pubsub_publisher.stop.messages.count', value=len(self._futures_buffer))
        self._publisher.stop()
        futures.wait(self._futures_buffer, return_when=futures.ALL_COMPLETED)


class SyncPubsubPublisherClient(BasePubsubPublisherClient):
    _publisher: pubsub_v1.PublisherClient

    def __init__(self):
        batch_settings = pubsub_v1.types.BatchSettings(max_messages=1)  # no batching
        self._publisher = pubsub_v1.PublisherClient(batch_settings=batch_settings)

    # Sync publish
    def publish(self, topic: str, data: bytes) -> None:
        future = self._publisher.publish(topic, data)
        future.result()

    def stop(self) -> None:
        pass
