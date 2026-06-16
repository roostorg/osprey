import json
import threading
import time
from typing import NoReturn

from osprey.worker.stress.producer import (
    Producer,
    ProducerConfig,
    _action_id_for,
    build_event,
)


class FakeKafkaProducer:
    """Captures sends in-memory; mimics the kafka-python API surface we use."""

    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs
        self.sent: list[tuple[str, bytes]] = []
        self._lock = threading.Lock()
        self.flush_called = False
        self.close_called = False

    def send(self, topic: str, value: bytes) -> None:
        with self._lock:
            self.sent.append((topic, value))

    def flush(self, timeout: float = 0) -> None:
        self.flush_called = True

    def close(self, timeout: float = 0) -> None:
        self.close_called = True


class TestActionIdGenerator:
    def test_returns_int(self) -> None:
        assert isinstance(_action_id_for('abcdef12', 0), int)

    def test_unique_per_event_within_run(self) -> None:
        ids = {_action_id_for('abcdef12', n) for n in range(1000)}
        assert len(ids) == 1000

    def test_different_runs_dont_collide_for_same_n(self) -> None:
        a = _action_id_for('11111111', 0)
        b = _action_id_for('22222222', 0)
        assert a != b

    def test_under_javascript_safe_integer(self) -> None:
        # 2**53 = 9007199254740992. Action_ids must fit so JSON consumers (Druid,
        # browsers) don't lose precision.
        for run_id in ['00000000', 'ffffffff']:
            for n in [0, 1_000_000, 9_999_999]:
                assert _action_id_for(run_id, n) < 2**53


class TestBuildEvent:
    def test_action_id_is_int(self) -> None:
        action_id, _ = build_event('abcdef12', 5)
        assert isinstance(action_id, int)

    def test_payload_shape_matches_template(self) -> None:
        action_id, raw = build_event('abcdef12', 7, now=1700000000.0)
        payload = json.loads(raw)
        # Must align with example_data/template.json so example_rules' SML can
        # extract fields by the same JSON paths.
        assert payload['data']['action_id'] == action_id
        assert isinstance(payload['data']['action_id'], int)
        assert payload['data']['action_name'] == 'create_post'
        assert payload['data']['data']['user_id'] == 'stress_user_7'
        assert payload['data']['data']['event_type'] == 'create_post'
        assert 'hello' in payload['data']['data']['post']['text']
        assert payload['send_time'].endswith('Z')

    def test_user_id_is_semantically_a_user_id(self) -> None:
        # Regression guard: don't overload user_id for tracking purposes.
        # Tracking is done via action_id + GetActionId() UDF — user_id stays
        # a plausible user identifier.
        _, raw = build_event('abcdef12', 7)
        payload = json.loads(raw)
        assert payload['data']['data']['user_id'].startswith('stress_user_')

    def test_deterministic_for_same_inputs(self) -> None:
        a_id, a_raw = build_event('abcdef12', 1, now=1700000000.0)
        b_id, b_raw = build_event('abcdef12', 1, now=1700000000.0)
        assert a_id == b_id
        assert a_raw == b_raw


class TestProducer:
    def test_produces_exact_count(self) -> None:
        fake = FakeKafkaProducer()
        config = ProducerConfig(
            bootstrap_servers=['ignored'],
            topic='osprey.actions_input',
            events=20,
            rate_per_second=1000.0,
            run_id='abcdef12',
        )
        producer = Producer(config, producer_factory=lambda **kw: fake)
        producer.start()
        producer.wait(timeout=5)
        assert producer.error is None
        assert len(fake.sent) == 20
        assert len(producer.produced) == 20

    def test_action_ids_unique_and_ordered(self) -> None:
        fake = FakeKafkaProducer()
        config = ProducerConfig(
            bootstrap_servers=['ignored'],
            topic='topic',
            events=10,
            rate_per_second=1000.0,
            run_id='abcdef12',
        )
        producer = Producer(config, producer_factory=lambda **kw: fake)
        producer.start()
        producer.wait(timeout=5)
        ids = list(producer.produced.keys())
        assert ids == [_action_id_for('abcdef12', n) for n in range(10)]

    def test_records_send_timestamp_per_event(self) -> None:
        fake = FakeKafkaProducer()
        config = ProducerConfig(
            bootstrap_servers=['ignored'],
            topic='topic',
            events=5,
            rate_per_second=1000.0,
            run_id='abcdef12',
        )
        producer = Producer(config, producer_factory=lambda **kw: fake)
        before = time.time()
        producer.start()
        producer.wait(timeout=5)
        after = time.time()
        for ts in producer.produced.values():
            assert before <= ts <= after

    def test_rate_control_approximate(self) -> None:
        fake = FakeKafkaProducer()
        config = ProducerConfig(
            bootstrap_servers=['ignored'],
            topic='topic',
            events=20,
            rate_per_second=100.0,
            run_id='abcdef12',
        )
        producer = Producer(config, producer_factory=lambda **kw: fake)
        start = time.monotonic()
        producer.start()
        producer.wait(timeout=5)
        elapsed = time.monotonic() - start
        assert 0.15 < elapsed < 1.0, f'elapsed={elapsed:.3f}s'

    def test_stop_aborts_early(self) -> None:
        fake = FakeKafkaProducer()
        config = ProducerConfig(
            bootstrap_servers=['ignored'],
            topic='topic',
            events=1000,
            rate_per_second=100.0,
            run_id='abcdef12',
        )
        producer = Producer(config, producer_factory=lambda **kw: fake)
        producer.start()
        time.sleep(0.1)
        producer.stop()
        producer.wait(timeout=5)
        assert len(fake.sent) < 1000
        assert fake.close_called

    def test_starting_twice_raises(self) -> None:
        fake = FakeKafkaProducer()
        config = ProducerConfig(
            bootstrap_servers=['ignored'],
            topic='topic',
            events=1,
            rate_per_second=1000.0,
            run_id='abcdef12',
        )
        producer = Producer(config, producer_factory=lambda **kw: fake)
        producer.start()
        try:
            producer.start()
            assert False, 'should have raised'
        except RuntimeError:
            pass
        producer.wait(timeout=2)

    def test_producer_factory_failure_captured(self) -> None:
        def boom(**_: object) -> NoReturn:
            raise ConnectionError('no kafka here')

        config = ProducerConfig(
            bootstrap_servers=['ignored'],
            topic='topic',
            events=10,
            rate_per_second=1000.0,
            run_id='abcdef12',
        )
        producer = Producer(config, producer_factory=boom)
        producer.start()
        producer.wait(timeout=2)
        assert isinstance(producer.error, ConnectionError)
        assert producer.produced == {}

    def test_run_id_helper(self) -> None:
        run_id = ProducerConfig.make_run_id()
        assert len(run_id) == 8
        assert all(c in '0123456789abcdef' for c in run_id)

    def test_close_runs_even_if_flush_raises(self) -> None:
        # Regression: flush() and close() were in one try, so a flush()
        # exception would skip close() and leak the socket.
        class FlushFailsProducer(FakeKafkaProducer):
            def flush(self, timeout: float = 0) -> None:
                raise RuntimeError('flush exploded')

        fake = FlushFailsProducer()
        config = ProducerConfig(
            bootstrap_servers=['ignored'],
            topic='topic',
            events=2,
            rate_per_second=1000.0,
            run_id='abcdef12',
        )
        producer = Producer(config, producer_factory=lambda **kw: fake)
        producer.start()
        producer.wait(timeout=5)
        assert fake.close_called
        assert isinstance(producer.error, RuntimeError)
        assert 'flush exploded' in str(producer.error)
