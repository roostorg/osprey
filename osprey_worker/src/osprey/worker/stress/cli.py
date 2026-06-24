"""Stress harness CLI entry point.

Usage:

    osprey-stress run \\
        --events 10000 --rate 1000 \\
        --threshold-drop-rate 0.01 --threshold-p95-ms 500 \\
        --report json

Run an end-to-end stress test against a live Osprey worker. Produces N events
to the input topic at rate R, consumes the resulting `ExecutionResult`s from
the output topic, and reports drop rate + latency. Exits non-zero on threshold
breach so it can be wired into CI as a gate.

The `measure` subcommand is reserved for when #236 (jetstream input stream
plugin) lands — it will let the same measurement layer run in open-loop mode
against any external input source. Today it prints a stub message.
"""

from __future__ import annotations

import argparse
import sys
import threading
import time
from collections.abc import Hashable, Mapping
from dataclasses import dataclass
from typing import Callable, Optional, Sequence, TextIO, cast

from kafka import KafkaConsumer
from kafka.structs import TopicPartition
from osprey.worker.stress.consumer import Consumer, ConsumerConfig
from osprey.worker.stress.producer import Producer, ProducerConfig, _action_id_for
from osprey.worker.stress.reporter import (
    EXIT_INTERNAL_ERROR,
    Thresholds,
    compute_report,
    exit_code_for,
)

DEFAULT_BOOTSTRAP = 'localhost:9092'
DEFAULT_INPUT_TOPIC = 'osprey.actions_input'
DEFAULT_OUTPUT_TOPIC = 'osprey.execution_results'


@dataclass(frozen=True)
class TopicSnapshot:
    """High-water-mark offsets for a topic at one point in time."""

    topic: str
    end_offsets: Mapping[int, int]

    @property
    def total(self) -> int:
        return sum(self.end_offsets.values())


def probe_topic_head(
    bootstrap_servers: list[str],
    topic: str,
    *,
    consumer_factory: Callable[..., KafkaConsumer] = KafkaConsumer,
) -> TopicSnapshot:
    """Snapshot a topic's current end-offsets without joining a consumer group.

    Used to surface drain progress and "prior runs left backlog" situations: the
    worker's processing rate is observable as the rate at which the output
    topic's head advances vs the rate the input topic's head advances. A topic
    that doesn't exist yet returns an empty snapshot rather than raising.
    """
    consumer = consumer_factory(
        bootstrap_servers=bootstrap_servers,
        group_id=None,
        client_id='osprey-stress-probe',
        consumer_timeout_ms=1000,
    )
    try:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            return TopicSnapshot(topic=topic, end_offsets={})
        tps = [TopicPartition(topic, p) for p in partitions]
        consumer.assign(tps)
        end_offsets = consumer.end_offsets(tps)
        return TopicSnapshot(topic=topic, end_offsets={tp.partition: off for tp, off in end_offsets.items()})
    finally:
        try:
            consumer.close(autocommit=False)
        except Exception:
            # Probe is best-effort. A noisy close() during teardown would mask
            # the snapshot value the caller needs.
            pass


class ProgressReporter:
    """Periodic stderr progress lines for an in-flight stress run.

    Decoupled from Consumer/Producer so they don't carry observability
    concerns. The constructor takes accessor callables for the live values it
    needs to print, so the same reporter works against the real consumer or a
    fake in tests.
    """

    def __init__(
        self,
        *,
        interval_seconds: float,
        target_events: int,
        get_produced: Callable[[], int],
        get_matched: Callable[[], int],
        probe_input: Callable[[], int],
        probe_output: Callable[[], int],
        output: TextIO = sys.stderr,
    ):
        self._interval = interval_seconds
        self._target = target_events
        self._get_produced = get_produced
        self._get_matched = get_matched
        self._probe_input = probe_input
        self._probe_output = probe_output
        self._output = output
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._initial_input = 0
        self._initial_output = 0
        self._start_monotonic = 0.0

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError('ProgressReporter already started')
        self._start_monotonic = time.monotonic()
        # Probe baselines on the calling thread so the first periodic emit can
        # diff against them without racing the worker's startup.
        self._initial_input = self._probe_input()
        self._initial_output = self._probe_output()
        self._thread = threading.Thread(target=self._run, name='stress-progress', daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self._interval + 1.0)

    def _run(self) -> None:
        # Event.wait returns True when set, False on timeout; we use it as an
        # interruptible sleep so stop() takes effect within `interval` seconds.
        while not self._stop.wait(self._interval):
            try:
                self._emit()
            except Exception:
                # A failed probe (broker hiccup, transient timeout) shouldn't
                # tear down the in-flight stress run. Skip the tick.
                pass

    def _emit(self) -> None:
        elapsed = max(time.monotonic() - self._start_monotonic, 0.001)
        produced = self._get_produced()
        matched = self._get_matched()
        in_delta = self._probe_input() - self._initial_input
        out_delta = self._probe_output() - self._initial_output
        match_rate = matched / elapsed
        print(
            f'[osprey-stress] t={elapsed:4.0f}s '
            f'produced={produced}/{self._target} matched={matched} '
            f'match_rate={match_rate:5.0f}/s '
            f'in_topic_Δ={in_delta} out_topic_Δ={out_delta}',
            file=self._output,
        )


def _positive_int(s: str) -> int:
    value = int(s)
    if value <= 0:
        raise argparse.ArgumentTypeError(f'must be > 0, got {value}')
    return value


def _positive_float(s: str) -> float:
    value = float(s)
    if value <= 0:
        raise argparse.ArgumentTypeError(f'must be > 0, got {value}')
    return value


def _nonneg_float(s: str) -> float:
    value = float(s)
    if value < 0:
        raise argparse.ArgumentTypeError(f'must be >= 0, got {value}')
    return value


def _add_common_kafka_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        '--bootstrap-servers',
        default=DEFAULT_BOOTSTRAP,
        help=f'Comma-separated Kafka bootstrap servers (default: {DEFAULT_BOOTSTRAP}).',
    )
    parser.add_argument(
        '--input-topic',
        default=DEFAULT_INPUT_TOPIC,
        help=f'Kafka topic to produce events to (default: {DEFAULT_INPUT_TOPIC}).',
    )
    parser.add_argument(
        '--output-topic',
        default=DEFAULT_OUTPUT_TOPIC,
        help=f'Kafka topic to read execution results from (default: {DEFAULT_OUTPUT_TOPIC}).',
    )


def _add_threshold_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        '--threshold-drop-rate',
        type=float,
        default=None,
        help='Exit non-zero if observed drop rate exceeds this (0.0–1.0).',
    )
    parser.add_argument(
        '--threshold-p95-ms',
        type=float,
        default=None,
        help='Exit non-zero if observed p95 round-trip latency exceeds this (ms).',
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog='osprey-stress',
        description=(
            'Produce synthetic events to Osprey, observe results, report drop '
            'rate and latency. Useful for validating dependency bumps, '
            'measuring throughput regressions, and gating CI on pipeline health.'
        ),
    )
    subparsers = parser.add_subparsers(dest='command', required=True)

    run = subparsers.add_parser(
        'run',
        help='Produce synthetic events and measure their round-trip (closed-loop).',
    )
    run.add_argument('--events', type=_positive_int, default=1000, help='Number of events to produce.')
    run.add_argument('--rate', type=_positive_float, default=100.0, help='Events per second.')
    run.add_argument(
        '--drain-seconds',
        type=_nonneg_float,
        default=30.0,
        help='Max wall-clock time to wait for in-flight events to be evaluated after producer finishes.',
    )
    run.add_argument(
        '--report',
        choices=('human', 'json'),
        default='human',
        help='Report format on stdout.',
    )
    run.add_argument(
        '--verbose',
        action='store_true',
        help='Emit periodic progress lines to stderr (live throughput, topic deltas).',
    )
    run.add_argument(
        '--verbose-interval-seconds',
        type=_positive_float,
        default=2.0,
        help='Seconds between verbose progress lines (default: 2.0). Ignored without --verbose.',
    )
    _add_common_kafka_args(run)
    _add_threshold_args(run)

    measure = subparsers.add_parser(
        'measure',
        help='Measure-only mode (open-loop). Stub until #236 lands.',
    )
    _add_common_kafka_args(measure)
    _add_threshold_args(measure)
    measure.add_argument('--duration', type=_positive_float, default=60.0)
    measure.add_argument('--report', choices=('human', 'json'), default='human')

    return parser


def _bootstrap_list(s: str) -> list[str]:
    return [host.strip() for host in s.split(',') if host.strip()]


def cmd_run(args: argparse.Namespace) -> int:
    run_id = ProducerConfig.make_run_id()
    expected_action_ids = frozenset(_action_id_for(run_id, n) for n in range(args.events))
    bootstrap_list = _bootstrap_list(args.bootstrap_servers)

    print(
        f'[osprey-stress] run_id={run_id} events={args.events} rate={args.rate:.0f}/s '
        f'bootstrap={args.bootstrap_servers} input={args.input_topic} output={args.output_topic}',
        file=sys.stderr,
    )

    # Snapshot input + output topic heads BEFORE the producer starts. This
    # baseline is the regression-across-runs signal: if input topic head is
    # already high (relative to expected throughput), the worker is still
    # working through events from a prior run, and the numbers from THIS run
    # will be misleading until that backlog drains.
    try:
        input_baseline = probe_topic_head(bootstrap_list, args.input_topic)
        output_baseline = probe_topic_head(bootstrap_list, args.output_topic)
    except Exception as e:
        print(f'[osprey-stress] could not probe topic heads ({e}); skipping baseline', file=sys.stderr)
        input_baseline = TopicSnapshot(args.input_topic, {})
        output_baseline = TopicSnapshot(args.output_topic, {})
    print(
        f'[osprey-stress] baseline: input_topic head={input_baseline.total} output_topic head={output_baseline.total}',
        file=sys.stderr,
    )

    consumer = Consumer(
        ConsumerConfig(
            bootstrap_servers=bootstrap_list,
            topic=args.output_topic,
            group_id=f'osprey-stress-{run_id}',
            action_id_filter=expected_action_ids,
            max_runtime_seconds=(args.events / args.rate) + args.drain_seconds,
            stop_when_filter_complete=True,
        )
    )
    consumer.start()
    # Give the consumer time to join the group and get a partition assignment
    # before the producer starts emitting. Without this, results from the
    # earliest events can be missed.
    time.sleep(2.0)
    if consumer.error is not None:
        print(f'[osprey-stress] consumer failed to start: {consumer.error}', file=sys.stderr)
        return EXIT_INTERNAL_ERROR

    producer = Producer(
        ProducerConfig(
            bootstrap_servers=bootstrap_list,
            topic=args.input_topic,
            events=args.events,
            rate_per_second=args.rate,
            run_id=run_id,
        )
    )

    reporter: Optional[ProgressReporter] = None
    if args.verbose:
        reporter = ProgressReporter(
            interval_seconds=args.verbose_interval_seconds,
            target_events=args.events,
            get_produced=lambda: len(producer.produced),
            get_matched=lambda: len(consumer.consumed),
            probe_input=lambda: probe_topic_head(bootstrap_list, args.input_topic).total,
            probe_output=lambda: probe_topic_head(bootstrap_list, args.output_topic).total,
        )
        reporter.start()

    start_wall = time.monotonic()
    producer.start()
    producer.wait()
    if producer.error is not None:
        consumer.stop()
        consumer.wait(timeout=5)
        if reporter is not None:
            reporter.stop()
        print(f'[osprey-stress] producer failed: {producer.error}', file=sys.stderr)
        return EXIT_INTERNAL_ERROR

    print('[osprey-stress] producer done, draining consumer...', file=sys.stderr)
    consumer.wait()  # bounded by max_runtime_seconds set above
    end_wall = time.monotonic()
    if reporter is not None:
        reporter.stop()

    if consumer.error is not None:
        print(f'[osprey-stress] consumer error: {consumer.error}', file=sys.stderr)
        return EXIT_INTERNAL_ERROR

    # Post-run snapshot: how far did each topic actually advance? If
    # output_topic advanced by far less than `args.events`, the worker is the
    # bottleneck (regardless of what the report says about THIS run's matches).
    try:
        input_after = probe_topic_head(bootstrap_list, args.input_topic)
        output_after = probe_topic_head(bootstrap_list, args.output_topic)
        in_delta = input_after.total - input_baseline.total
        out_delta = output_after.total - output_baseline.total
        print(
            f'[osprey-stress] topic deltas: input +{in_delta} (expected ~{args.events}) output +{out_delta}',
            file=sys.stderr,
        )
        if out_delta < args.events:
            shortfall = args.events - out_delta
            print(
                f'[osprey-stress] worker emitted {shortfall} fewer results than events sent — '
                f'pipeline backpressure; next run will start with this backlog still in flight',
                file=sys.stderr,
            )
    except Exception as e:
        print(f'[osprey-stress] could not probe post-run topic heads ({e})', file=sys.stderr)

    # cast: `dict[int, float]` is a valid `Mapping[Hashable, float]` at runtime
    # (int is Hashable), but mypy treats `Mapping` keys as invariant so the
    # auto-conversion doesn't fly. The cast is honest.
    report = compute_report(
        produced=cast(Mapping[Hashable, float], producer.produced),
        consumed=cast(Mapping[Hashable, float], consumer.consumed),
        duration_seconds=end_wall - start_wall,
        thresholds=Thresholds(drop_rate=args.threshold_drop_rate, p95_ms=args.threshold_p95_ms),
    )

    if args.report == 'json':
        print(report.to_json())
    else:
        print(report.to_human())

    return exit_code_for(report)


def cmd_measure(args: argparse.Namespace) -> int:
    print(
        '[osprey-stress] `measure` is not yet implemented. It will activate once '
        '#236 (jetstream input stream plugin) lands so this CLI can run the '
        'measurement layer against externally-produced events. '
        'Use `osprey-stress run` for synthetic closed-loop testing.',
        file=sys.stderr,
    )
    return EXIT_INTERNAL_ERROR


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == 'run':
        return cmd_run(args)
    if args.command == 'measure':
        return cmd_measure(args)
    parser.print_help(sys.stderr)
    return EXIT_INTERNAL_ERROR


if __name__ == '__main__':
    sys.exit(main())
