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
import time
from collections.abc import Hashable, Mapping
from typing import Sequence, cast

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
    run.add_argument('--events', type=int, default=1000, help='Number of events to produce.')
    run.add_argument('--rate', type=float, default=100.0, help='Events per second.')
    run.add_argument(
        '--drain-seconds',
        type=float,
        default=30.0,
        help='Max wall-clock time to wait for in-flight events to be evaluated after producer finishes.',
    )
    run.add_argument(
        '--report',
        choices=('human', 'json'),
        default='human',
        help='Report format on stdout.',
    )
    _add_common_kafka_args(run)
    _add_threshold_args(run)

    measure = subparsers.add_parser(
        'measure',
        help='Measure-only mode (open-loop). Stub until #236 lands.',
    )
    _add_common_kafka_args(measure)
    _add_threshold_args(measure)
    measure.add_argument('--duration', type=float, default=60.0)
    measure.add_argument('--report', choices=('human', 'json'), default='human')

    return parser


def _bootstrap_list(s: str) -> list[str]:
    return [host.strip() for host in s.split(',') if host.strip()]


def cmd_run(args: argparse.Namespace) -> int:
    run_id = ProducerConfig.make_run_id()
    expected_action_ids = frozenset(_action_id_for(run_id, n) for n in range(args.events))

    print(
        f'[osprey-stress] run_id={run_id} events={args.events} rate={args.rate:.0f}/s '
        f'bootstrap={args.bootstrap_servers} input={args.input_topic} output={args.output_topic}',
        file=sys.stderr,
    )

    consumer = Consumer(
        ConsumerConfig(
            bootstrap_servers=_bootstrap_list(args.bootstrap_servers),
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
            bootstrap_servers=_bootstrap_list(args.bootstrap_servers),
            topic=args.input_topic,
            events=args.events,
            rate_per_second=args.rate,
            run_id=run_id,
        )
    )
    start_wall = time.monotonic()
    producer.start()
    producer.wait()
    if producer.error is not None:
        consumer.stop()
        consumer.wait(timeout=5)
        print(f'[osprey-stress] producer failed: {producer.error}', file=sys.stderr)
        return EXIT_INTERNAL_ERROR

    print('[osprey-stress] producer done, draining consumer...', file=sys.stderr)
    consumer.wait()  # bounded by max_runtime_seconds set above
    end_wall = time.monotonic()

    if consumer.error is not None:
        print(f'[osprey-stress] consumer error: {consumer.error}', file=sys.stderr)
        return EXIT_INTERNAL_ERROR

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
