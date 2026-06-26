import io
import time
from unittest.mock import MagicMock

import pytest
from kafka.structs import TopicPartition
from osprey.worker.stress.cli import (
    EXIT_INTERNAL_ERROR,
    ProgressReporter,
    TopicSnapshot,
    build_parser,
    cmd_measure,
    main,
    probe_topic_head,
)


class TestParser:
    def test_run_minimal_args_parse(self) -> None:
        args = build_parser().parse_args(['run'])
        assert args.command == 'run'
        assert args.events == 1000
        assert args.rate == 100.0
        assert args.report == 'human'

    def test_run_threshold_args_parse(self) -> None:
        args = build_parser().parse_args(
            [
                'run',
                '--events',
                '50',
                '--rate',
                '10',
                '--threshold-drop-rate',
                '0.05',
                '--threshold-p95-ms',
                '750',
                '--report',
                'json',
            ]
        )
        assert args.events == 50
        assert args.rate == 10.0
        assert args.threshold_drop_rate == 0.05
        assert args.threshold_p95_ms == 750.0
        assert args.report == 'json'

    def test_run_kafka_args_parse(self) -> None:
        args = build_parser().parse_args(
            [
                'run',
                '--bootstrap-servers',
                'kafka-a:9092,kafka-b:9092',
                '--input-topic',
                'my.input',
                '--output-topic',
                'my.output',
            ]
        )
        assert args.bootstrap_servers == 'kafka-a:9092,kafka-b:9092'
        assert args.input_topic == 'my.input'
        assert args.output_topic == 'my.output'

    def test_no_command_requires_one(self) -> None:
        with pytest.raises(SystemExit):
            build_parser().parse_args([])

    @pytest.mark.parametrize(
        'argv',
        [
            ['run', '--events', '0'],
            ['run', '--events', '-5'],
            ['run', '--rate', '0'],
            ['run', '--rate', '-1.5'],
            ['run', '--drain-seconds', '-0.1'],
            ['measure', '--duration', '0'],
            ['measure', '--duration', '-10'],
        ],
    )
    def test_rejects_non_positive_numeric_args(self, argv: list[str]) -> None:
        # Regression: previously the parser accepted 0 / negatives, which led
        # to ZeroDivisionError later when computing max_runtime_seconds.
        with pytest.raises(SystemExit):
            build_parser().parse_args(argv)


class TestMeasureStub:
    def test_measure_returns_internal_error_for_now(self, capsys: pytest.CaptureFixture[str]) -> None:
        args = build_parser().parse_args(['measure'])
        rc = cmd_measure(args)
        assert rc == EXIT_INTERNAL_ERROR
        captured = capsys.readouterr()
        assert '#236' in captured.err

    def test_main_dispatches_to_measure(self) -> None:
        rc = main(['measure'])
        assert rc == EXIT_INTERNAL_ERROR


class TestVerboseFlagParsing:
    def test_verbose_defaults_off(self) -> None:
        args = build_parser().parse_args(['run'])
        assert args.verbose is False
        assert args.verbose_interval_seconds == 2.0

    def test_verbose_flag_sets_true(self) -> None:
        args = build_parser().parse_args(['run', '--verbose'])
        assert args.verbose is True

    def test_verbose_interval_custom(self) -> None:
        args = build_parser().parse_args(['run', '--verbose', '--verbose-interval-seconds', '0.5'])
        assert args.verbose_interval_seconds == 0.5

    def test_verbose_interval_rejects_non_positive(self) -> None:
        with pytest.raises(SystemExit):
            build_parser().parse_args(['run', '--verbose-interval-seconds', '0'])


def _fake_kafka_consumer(partitions: set[int], end_offsets: dict[int, int]) -> MagicMock:
    """Build a MagicMock that mimics enough of `KafkaConsumer` for `probe_topic_head`."""
    fake = MagicMock()
    fake.partitions_for_topic.return_value = partitions
    fake.end_offsets.side_effect = lambda tps: {tp: end_offsets[tp.partition] for tp in tps}
    return fake


class TestProbeTopicHead:
    def test_sums_partition_offsets(self) -> None:
        fake = _fake_kafka_consumer({0, 1, 2}, {0: 100, 1: 250, 2: 75})
        snapshot = probe_topic_head(['localhost:9092'], 'osprey.actions_input', consumer_factory=lambda **_: fake)
        assert snapshot.topic == 'osprey.actions_input'
        assert snapshot.end_offsets == {0: 100, 1: 250, 2: 75}
        assert snapshot.total == 425
        fake.close.assert_called_once()

    def test_empty_when_topic_missing(self) -> None:
        # partitions_for_topic returns None for nonexistent topics — the probe
        # tolerates this so a brand-new cluster doesn't crash the CLI.
        fake = MagicMock()
        fake.partitions_for_topic.return_value = None
        snapshot = probe_topic_head(['localhost:9092'], 'no.such.topic', consumer_factory=lambda **_: fake)
        assert snapshot.total == 0
        assert snapshot.end_offsets == {}
        fake.assign.assert_not_called()

    def test_close_failure_is_swallowed(self) -> None:
        fake = _fake_kafka_consumer({0}, {0: 10})
        fake.close.side_effect = RuntimeError('broker went away')
        # Should not raise — probe is best-effort.
        snapshot = probe_topic_head(['localhost:9092'], 't', consumer_factory=lambda **_: fake)
        assert snapshot.total == 10

    def test_assigns_topic_partitions(self) -> None:
        fake = _fake_kafka_consumer({0, 1}, {0: 5, 1: 8})
        probe_topic_head(['localhost:9092'], 'my.topic', consumer_factory=lambda **_: fake)
        assigned: list[TopicPartition] = fake.assign.call_args.args[0]
        assert {tp.partition for tp in assigned} == {0, 1}
        assert all(tp.topic == 'my.topic' for tp in assigned)


class TestProgressReporter:
    def test_emits_periodic_progress_with_topic_deltas(self) -> None:
        # Counters the reporter pulls from each tick. Producer climbs, matched
        # lags behind, topics advance — the line should include all four signals.
        produced = [0]
        matched = [0]
        input_offset = [1000]
        output_offset = [500]

        def tick(p: int, m: int, in_o: int, out_o: int) -> None:
            produced[0] = p
            matched[0] = m
            input_offset[0] = in_o
            output_offset[0] = out_o

        sink = io.StringIO()
        reporter = ProgressReporter(
            interval_seconds=0.05,
            target_events=100,
            get_produced=lambda: produced[0],
            get_matched=lambda: matched[0],
            probe_input=lambda: input_offset[0],
            probe_output=lambda: output_offset[0],
            output=sink,
        )
        reporter.start()
        tick(50, 30, 1100, 550)
        time.sleep(0.15)
        tick(100, 70, 1200, 600)
        time.sleep(0.10)
        reporter.stop()

        lines = [ln for ln in sink.getvalue().splitlines() if ln]
        assert lines, 'expected at least one progress line'
        last = lines[-1]
        assert 'produced=' in last
        assert 'matched=' in last
        assert 'in_topic_Δ=' in last
        assert 'out_topic_Δ=' in last

    def test_double_start_raises(self) -> None:
        reporter = ProgressReporter(
            interval_seconds=1.0,
            target_events=1,
            get_produced=lambda: 0,
            get_matched=lambda: 0,
            probe_input=lambda: 0,
            probe_output=lambda: 0,
            output=io.StringIO(),
        )
        reporter.start()
        try:
            with pytest.raises(RuntimeError):
                reporter.start()
        finally:
            reporter.stop()

    def test_probe_failure_does_not_kill_reporter(self) -> None:
        # Transient broker errors during a probe shouldn't tear down the run.
        calls = [0]

        def flaky_probe() -> int:
            calls[0] += 1
            if calls[0] % 2 == 0:
                raise RuntimeError('transient kafka error')
            return calls[0]

        sink = io.StringIO()
        reporter = ProgressReporter(
            interval_seconds=0.05,
            target_events=10,
            get_produced=lambda: 0,
            get_matched=lambda: 0,
            probe_input=flaky_probe,
            probe_output=lambda: 0,
            output=sink,
        )
        reporter.start()
        time.sleep(0.25)
        reporter.stop()
        # Reporter should still be running (not crashed) — verified by stop
        # succeeding cleanly and at least one line landing in the sink.
        assert sink.getvalue()


class TestTopicSnapshot:
    def test_total_sums_partition_offsets(self) -> None:
        snap = TopicSnapshot(topic='t', end_offsets={0: 10, 1: 20, 2: 5})
        assert snap.total == 35

    def test_total_zero_for_empty(self) -> None:
        assert TopicSnapshot(topic='t', end_offsets={}).total == 0
