"""Threaded wrappers for confluent-kafka that keep librdkafka's C threads off the gevent event loop.

confluent-kafka uses librdkafka (C) under the hood. Its internal threads and network I/O bypass
gevent's monkey-patching entirely, which means they can hold the GIL and starve greenlets. These
wrappers run all confluent-kafka operations on dedicated OS threads and bridge back to greenlet-land
via gevent queues.
"""

import threading
from typing import Any, Optional

import gevent.queue
import sentry_sdk
from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer
from osprey.worker.lib.osprey_shared.logging import get_logger

logger = get_logger()

_SENTINEL = object()


class ThreadedKafkaConsumer:
    """Wraps a confluent-kafka Consumer, polling on a real OS thread and delivering messages via a gevent queue."""

    def __init__(self, consumer: Consumer, queue_maxsize: int = 1000) -> None:
        self._consumer = consumer
        self._queue: gevent.queue.Queue[Optional[Message]] = gevent.queue.Queue(maxsize=queue_maxsize)
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._poll_loop, daemon=True, name='kafka-consumer-thread')
        self._thread.start()

    def _poll_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                msg = self._consumer.poll(timeout=1.0)
                if msg is not None:
                    self._queue.put(msg)
            except KafkaException as e:
                logger.error(f'Kafka consumer poll error: {e}')
                sentry_sdk.capture_exception(e)

    def poll(self, timeout: Optional[float] = None) -> Optional[Message]:
        """Get the next message from the gevent queue. Safe to call from a greenlet."""
        try:
            return self._queue.get(timeout=timeout)
        except gevent.queue.Empty:
            return None

    def close(self) -> None:
        self._stop_event.set()
        self._thread.join(timeout=5)
        self._consumer.close()


class ThreadedKafkaProducer:
    """Wraps a confluent-kafka Producer, running produce/poll on a real OS thread."""

    def __init__(self, producer: Producer, poll_interval: float = 0.1) -> None:
        self._producer = producer
        self._queue: gevent.queue.Queue = gevent.queue.Queue()
        self._poll_interval = poll_interval
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._produce_loop, daemon=True, name='kafka-producer-thread')
        self._thread.start()

    def _produce_loop(self) -> None:
        while not self._stop_event.is_set():
            self._producer.poll(0)

            try:
                item = self._queue.get_nowait()
            except gevent.queue.Empty:
                # No pending produce requests — just poll and sleep briefly
                self._stop_event.wait(self._poll_interval)
                continue

            if item is _SENTINEL:
                break

            topic, value, on_delivery = item
            try:
                self._producer.produce(topic, value=value, on_delivery=on_delivery)
            except BufferError:
                logger.warning('Producer queue full, flushing before retry')
                self._producer.flush(timeout=5)
                self._producer.produce(topic, value=value, on_delivery=on_delivery)

    def produce(self, topic: str, value: bytes, on_delivery: Any = None) -> None:
        """Queue a message for production. Safe to call from a greenlet."""
        self._queue.put((topic, value, on_delivery))

    def flush(self, timeout: float = 30) -> int:
        """Flush pending messages. Blocks until complete."""
        return self._producer.flush(timeout)

    def close(self, timeout: float = 10) -> int:
        """Stop the producer thread and flush remaining messages."""
        self._stop_event.set()
        self._queue.put(_SENTINEL)
        self._thread.join(timeout=5)
        return self._producer.flush(timeout)
