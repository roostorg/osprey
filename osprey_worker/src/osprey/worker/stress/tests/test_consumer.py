import json
import threading
import time
from typing import Any, Iterator, List

from osprey.worker.stress.consumer import Consumer, ConsumerConfig


class FakeMessage:
    def __init__(self, value: bytes) -> None:
        self.value = value


class FakeKafkaConsumer:
    """In-memory consumer. Push messages via `feed()`; raises StopIteration
    on each idle poll cycle (matching kafka-python's `consumer_timeout_ms` shape)."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._lock = threading.Lock()
        self._queue: List[FakeMessage] = []
        self.kwargs = kwargs
        self.close_called = False

    def feed(self, raw: bytes) -> None:
        with self._lock:
            self._queue.append(FakeMessage(raw))

    def __iter__(self) -> Iterator[FakeMessage]:
        return self

    def __next__(self) -> FakeMessage:
        with self._lock:
            if self._queue:
                return self._queue.pop(0)
        raise StopIteration

    def close(self, autocommit: bool = True) -> None:
        self.close_called = True


def make_result_payload(action_id: int) -> bytes:
    return json.dumps({'ActionId': action_id, 'ActionName': 'create_post'}).encode('utf-8')


class TestConsumerClosedLoop:
    def test_records_matching_action_ids(self) -> None:
        fake = FakeKafkaConsumer()
        config = ConsumerConfig(
            bootstrap_servers=['ignored'],
            topic='osprey.execution_results',
            group_id='test',
            action_id_filter=frozenset({1, 2, 3}),
            max_runtime_seconds=2.0,
        )
        consumer = Consumer(config, consumer_factory=lambda *a, **kw: fake)
        consumer.start()
        for aid in [1, 2, 3]:
            fake.feed(make_result_payload(aid))
        consumer.wait(timeout=3)
        assert set(consumer.consumed.keys()) == {1, 2, 3}

    def test_ignores_non_matching_action_ids(self) -> None:
        fake = FakeKafkaConsumer()
        config = ConsumerConfig(
            bootstrap_servers=['ignored'],
            topic='topic',
            group_id='test',
            action_id_filter=frozenset({1, 2}),
            max_runtime_seconds=1.0,
        )
        consumer = Consumer(config, consumer_factory=lambda *a, **kw: fake)
        consumer.start()
        # Feed traffic that doesn't belong to this run.
        for aid in [99, 100, 101]:
            fake.feed(make_result_payload(aid))
        time.sleep(0.5)
        consumer.stop()
        consumer.wait(timeout=2)
        assert consumer.consumed == {}

    def test_stops_early_when_filter_complete(self) -> None:
        fake = FakeKafkaConsumer()
        config = ConsumerConfig(
            bootstrap_servers=['ignored'],
            topic='topic',
            group_id='test',
            action_id_filter=frozenset({1, 2, 3}),
            max_runtime_seconds=60.0,  # would block for a long time
            stop_when_filter_complete=True,
        )
        consumer = Consumer(config, consumer_factory=lambda *a, **kw: fake)
        consumer.start()
        for aid in [1, 2, 3]:
            fake.feed(make_result_payload(aid))
        start = time.monotonic()
        consumer.wait(timeout=10)
        elapsed = time.monotonic() - start
        # Stops well under max_runtime once it has matched the whole filter.
        assert elapsed < 5.0
        assert set(consumer.consumed.keys()) == {1, 2, 3}

    def test_first_write_wins_for_duplicate_action_ids(self) -> None:
        fake = FakeKafkaConsumer()
        config = ConsumerConfig(
            bootstrap_servers=['ignored'],
            topic='topic',
            group_id='test',
            action_id_filter=frozenset({1}),
            max_runtime_seconds=2.0,
            stop_when_filter_complete=False,
        )
        consumer = Consumer(config, consumer_factory=lambda *a, **kw: fake)
        consumer.start()
        fake.feed(make_result_payload(1))
        # Poll until the first message has actually been recorded — sleeping a
        # fixed interval can race on slow CI, leaving first_ts=None and turning
        # the equality check below into a meaningless assertion.
        deadline = time.monotonic() + 2.0
        while consumer.consumed.get(1) is None and time.monotonic() < deadline:
            time.sleep(0.05)
        first_ts = consumer.consumed.get(1)
        assert first_ts is not None, 'consumer never recorded the first message'
        fake.feed(make_result_payload(1))
        # Give the consumer a chance to (incorrectly) overwrite if first-write-wins
        # were broken. 0.5s is plenty since the poll cycle is 200ms.
        time.sleep(0.5)
        consumer.stop()
        consumer.wait(timeout=2)
        assert consumer.consumed[1] == first_ts


class TestConsumerOpenLoop:
    def test_counts_everything_without_filter(self) -> None:
        fake = FakeKafkaConsumer()
        config = ConsumerConfig(
            bootstrap_servers=['ignored'],
            topic='topic',
            group_id='test',
            action_id_filter=None,
            max_runtime_seconds=1.0,
        )
        consumer = Consumer(config, consumer_factory=lambda *a, **kw: fake)
        consumer.start()
        for aid in [10, 20, 30, 40, 50]:
            fake.feed(make_result_payload(aid))
        consumer.wait(timeout=3)
        assert set(consumer.consumed.keys()) == {10, 20, 30, 40, 50}


class TestConsumerMessageHygiene:
    def test_skips_malformed_json(self) -> None:
        fake = FakeKafkaConsumer()
        config = ConsumerConfig(
            bootstrap_servers=['ignored'],
            topic='topic',
            group_id='test',
            action_id_filter=None,
            max_runtime_seconds=1.0,
        )
        consumer = Consumer(config, consumer_factory=lambda *a, **kw: fake)
        consumer.start()
        fake.feed(b'not json{{{')
        fake.feed(make_result_payload(42))
        consumer.wait(timeout=3)
        assert consumer.consumed == {42: consumer.consumed[42]}

    def test_skips_missing_action_id_field(self) -> None:
        fake = FakeKafkaConsumer()
        config = ConsumerConfig(
            bootstrap_servers=['ignored'],
            topic='topic',
            group_id='test',
            action_id_filter=None,
            max_runtime_seconds=1.0,
        )
        consumer = Consumer(config, consumer_factory=lambda *a, **kw: fake)
        consumer.start()
        fake.feed(json.dumps({'OtherField': 'no action id here'}).encode('utf-8'))
        fake.feed(make_result_payload(7))
        consumer.wait(timeout=3)
        assert set(consumer.consumed.keys()) == {7}

    def test_skips_non_int_action_id(self) -> None:
        # Belt-and-suspenders: if a rule ever emits ActionId as a string,
        # we shouldn't crash; we just don't match it.
        fake = FakeKafkaConsumer()
        config = ConsumerConfig(
            bootstrap_servers=['ignored'],
            topic='topic',
            group_id='test',
            action_id_filter=None,
            max_runtime_seconds=1.0,
        )
        consumer = Consumer(config, consumer_factory=lambda *a, **kw: fake)
        consumer.start()
        fake.feed(json.dumps({'ActionId': 'not-an-int'}).encode('utf-8'))
        consumer.wait(timeout=3)
        assert consumer.consumed == {}


class TestConsumerLifecycle:
    def test_starting_twice_raises(self) -> None:
        fake = FakeKafkaConsumer()
        config = ConsumerConfig(
            bootstrap_servers=['ignored'],
            topic='topic',
            group_id='test',
            max_runtime_seconds=0.5,
        )
        consumer = Consumer(config, consumer_factory=lambda *a, **kw: fake)
        consumer.start()
        try:
            consumer.start()
            assert False, 'should have raised'
        except RuntimeError:
            pass
        consumer.wait(timeout=2)

    def test_factory_failure_captured(self) -> None:
        def boom(*_: Any, **__: Any) -> Any:
            raise ConnectionError('no kafka')

        config = ConsumerConfig(
            bootstrap_servers=['ignored'],
            topic='topic',
            group_id='test',
        )
        consumer = Consumer(config, consumer_factory=boom)
        consumer.start()
        consumer.wait(timeout=2)
        assert isinstance(consumer.error, ConnectionError)
        assert consumer.consumed == {}

    def test_respects_max_runtime(self) -> None:
        fake = FakeKafkaConsumer()
        config = ConsumerConfig(
            bootstrap_servers=['ignored'],
            topic='topic',
            group_id='test',
            action_id_filter=frozenset({1, 2, 3}),  # never fed → never completes
            max_runtime_seconds=0.5,
        )
        consumer = Consumer(config, consumer_factory=lambda *a, **kw: fake)
        start = time.monotonic()
        consumer.start()
        consumer.wait(timeout=3)
        elapsed = time.monotonic() - start
        assert 0.4 < elapsed < 2.0
