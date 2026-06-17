"""Async input streams for the async worker."""

import abc
import asyncio
import json
import logging
from collections import deque
from typing import Any, AsyncIterator, Generic, Mapping, Protocol, Sequence, TypeVar

from osprey.engine.executor.execution_context import Action
from osprey.worker.lib.utils.dates import parse_go_timestamp
from osprey.worker.sinks.utils.acking_contexts_base import BaseAckingContext, NoopAckingContext

_T = TypeVar('_T')

logger = logging.getLogger(__name__)


class AsyncBaseInputStream(abc.ABC, Generic[_T]):
    """Async version of BaseInputStream. Uses async iteration."""

    def __aiter__(self) -> AsyncIterator[_T]:
        return self._gen()

    @abc.abstractmethod
    async def _gen(self) -> AsyncIterator[_T]:
        raise NotImplementedError
        yield  # make this an async generator

    async def stop(self) -> None:
        pass


class AsyncStaticInputStream(AsyncBaseInputStream[_T]):
    """An async input stream that returns a static list, until exhausted. For testing."""

    def __init__(self, items: Sequence[_T]):
        self._items = deque(items)

    async def _gen(self) -> AsyncIterator[_T]:
        for item in self._items:
            yield item


class _PollableConsumer(Protocol):
    """The slice of kafka.KafkaConsumer this stream needs.

    Declaring it as a Protocol keeps this module free of a hard `kafka` import
    (the async worker forbids gevent, and kafka-python's patched consumer pulls
    it in) and makes the stream trivially testable with a fake consumer.
    """

    def poll(self, timeout_ms: int = ..., max_records: int = ...) -> Mapping[Any, Sequence[Any]]: ...

    def commit(self) -> None: ...

    def close(self) -> None: ...


class AsyncKafkaInputStream(AsyncBaseInputStream[BaseAckingContext[Action]]):
    """Consume Osprey-format actions from Kafka on the asyncio event loop.

    Decodes the same envelope the gevent ``KafkaInputStream`` reads:
    ``{"send_time": ..., "data": {"action_id", "action_name", "data": {...}}}``.

    kafka-python's consumer is blocking, so each poll runs in a worker thread
    via ``asyncio.to_thread``; the loop stays responsive and the bounded poll
    timeout lets ``stop()`` interrupt between polls. The consumer is injected so
    this module needs neither the kafka client nor any gevent-tainted helper.

    Offsets are committed manually, once per polled batch, only after every
    record in that batch has been handed back to (and processed by) the
    consumer of this generator. With ``enable_auto_commit=False`` this gives
    at-least-once delivery: a crash mid-batch reprocesses the batch rather than
    losing actions to an offset that was auto-committed before processing.
    """

    def __init__(
        self,
        consumer: _PollableConsumer,
        poll_timeout_ms: int = 1000,
        max_records: int = 500,
    ):
        self._consumer = consumer
        self._poll_timeout_ms = poll_timeout_ms
        self._max_records = max_records
        self._stopped = False

    @staticmethod
    def decode(record_value: Any) -> Action:
        """Decode a Kafka record value (bytes/str) into an Action."""
        data = json.loads(record_value)
        action_data = data['data']
        return Action(
            action_id=int(action_data['action_id']),
            action_name=action_data['action_name'],
            data=action_data['data'],
            timestamp=parse_go_timestamp(data['send_time']),
        )

    async def _gen(self) -> AsyncIterator[BaseAckingContext[Action]]:
        while not self._stopped:
            batches = await asyncio.to_thread(
                self._consumer.poll,
                timeout_ms=self._poll_timeout_ms,
                max_records=self._max_records,
            )
            for records in batches.values():
                for record in records:
                    try:
                        action = self.decode(record.value)
                    except Exception:
                        logger.exception('Error decoding Kafka record; skipping')
                        continue
                    yield NoopAckingContext(action)
            # Reached only after the generator resumes past the last yielded
            # record of this batch, i.e. once the batch has been processed.
            if batches and not self._stopped:
                await asyncio.to_thread(self._consumer.commit)

    async def stop(self) -> None:
        self._stopped = True
        await asyncio.to_thread(self._consumer.close)
