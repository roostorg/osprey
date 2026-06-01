"""Stress harness reporting.

Pure functions that consume produced/consumed event maps and emit a structured
report plus an exit code. No Kafka, no I/O. Tested in isolation under
`tests/test_reporter.py`.

The harness has two measurement modes:

* Closed-loop (synthetic source): we know every action_id we sent and when we
  sent it, so we can compute exact drop rate and per-event latency.
* Open-loop (external source — e.g. jetstream once #236 lands): we don't have
  per-event produce timestamps, so latency is unavailable; we report aggregate
  throughput and drop rate via input/output counts.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any, Mapping, Optional


@dataclass(frozen=True)
class LatencyStats:
    min_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float
    count: int


@dataclass(frozen=True)
class Thresholds:
    drop_rate: Optional[float] = None
    p95_ms: Optional[float] = None


@dataclass(frozen=True)
class Report:
    mode: str  # "closed-loop" | "open-loop"
    produced: int
    consumed: int
    matched: int
    drop_count: int
    drop_rate: float
    duration_seconds: float
    latency: Optional[LatencyStats] = None
    thresholds: Thresholds = field(default_factory=Thresholds)
    threshold_breaches: tuple[str, ...] = field(default_factory=tuple)

    def to_json(self) -> str:
        payload = asdict(self)
        return json.dumps(payload, indent=2, sort_keys=True)

    def to_human(self) -> str:
        lines = [
            f'mode:         {self.mode}',
            f'produced:     {self.produced}',
            f'consumed:     {self.consumed}',
            f'matched:      {self.matched}',
            f'drop count:   {self.drop_count}',
            f'drop rate:    {self.drop_rate:.4f}',
            f'duration:     {self.duration_seconds:.2f}s',
        ]
        if self.latency is not None:
            lines += [
                'latency (ms):',
                f'  min: {self.latency.min_ms:.1f}',
                f'  p50: {self.latency.p50_ms:.1f}',
                f'  p95: {self.latency.p95_ms:.1f}',
                f'  p99: {self.latency.p99_ms:.1f}',
                f'  max: {self.latency.max_ms:.1f}',
            ]
        if self.threshold_breaches:
            lines.append('breaches: ' + ', '.join(self.threshold_breaches))
        return '\n'.join(lines)


def percentile(sorted_values: list[float], p: float) -> float:
    """Nearest-rank percentile over a pre-sorted list. p in [0, 100]."""
    if not sorted_values:
        raise ValueError('percentile of empty list')
    if not 0 <= p <= 100:
        raise ValueError(f'p must be in [0, 100], got {p}')
    # Nearest-rank: ceil(p/100 * n) - 1, clamped.
    n = len(sorted_values)
    if p == 0:
        return sorted_values[0]
    rank = int(-(-p * n // 100)) - 1  # ceil division
    rank = max(0, min(rank, n - 1))
    return sorted_values[rank]


def compute_latency_stats(latencies_ms: list[float]) -> LatencyStats:
    sorted_ms = sorted(latencies_ms)
    return LatencyStats(
        min_ms=sorted_ms[0],
        p50_ms=percentile(sorted_ms, 50),
        p95_ms=percentile(sorted_ms, 95),
        p99_ms=percentile(sorted_ms, 99),
        max_ms=sorted_ms[-1],
        count=len(sorted_ms),
    )


def compute_report(
    *,
    produced: Mapping[Any, float],
    consumed: Mapping[Any, float],
    duration_seconds: float,
    thresholds: Thresholds = Thresholds(),
) -> Report:
    """Build a Report from per-event produce / consume timestamps.

    Keys can be any hashable type (int action_ids today; str in earlier
    iterations). Only set membership and ordering matter.

    Args:
        produced: id -> wall-clock timestamp (seconds) at which we sent the event.
                  Empty in open-loop mode.
        consumed: id -> wall-clock timestamp (seconds) at which we observed
                  the event's execution result.
        duration_seconds: wall-clock time the harness ran.
        thresholds: pass/fail gates. None entries are not enforced.
    """
    is_closed_loop = bool(produced)
    mode = 'closed-loop' if is_closed_loop else 'open-loop'

    produced_count = len(produced)
    consumed_count = len(consumed)

    if is_closed_loop:
        matched_ids = produced.keys() & consumed.keys()
        matched = len(matched_ids)
        drop_count = produced_count - matched
        drop_rate = drop_count / produced_count if produced_count else 0.0
        latencies = [(consumed[aid] - produced[aid]) * 1000 for aid in matched_ids]
        latency = compute_latency_stats(latencies) if latencies else None
    else:
        # Open-loop: best we can do without per-event produce timestamps is
        # treat consumed as our denominator. Drop accounting needs an external
        # source-of-truth count, which the caller would have to supply via a
        # different code path. For now, report no drops.
        matched = consumed_count
        drop_count = 0
        drop_rate = 0.0
        latency = None

    breaches: list[str] = []
    if thresholds.drop_rate is not None and drop_rate > thresholds.drop_rate:
        breaches.append(f'drop_rate {drop_rate:.4f} > {thresholds.drop_rate:.4f}')
    if thresholds.p95_ms is not None and latency is not None and latency.p95_ms > thresholds.p95_ms:
        breaches.append(f'p95_ms {latency.p95_ms:.1f} > {thresholds.p95_ms:.1f}')

    return Report(
        mode=mode,
        produced=produced_count,
        consumed=consumed_count,
        matched=matched,
        drop_count=drop_count,
        drop_rate=drop_rate,
        duration_seconds=duration_seconds,
        latency=latency,
        thresholds=thresholds,
        threshold_breaches=tuple(breaches),
    )


# Exit codes used by the CLI.
EXIT_OK = 0
EXIT_DROP_RATE_EXCEEDED = 1
EXIT_LATENCY_EXCEEDED = 2
EXIT_INTERNAL_ERROR = 3


def exit_code_for(report: Report) -> int:
    """Map report breaches to a CLI exit code."""
    for breach in report.threshold_breaches:
        if breach.startswith('drop_rate'):
            return EXIT_DROP_RATE_EXCEEDED
        if breach.startswith('p95_ms'):
            return EXIT_LATENCY_EXCEEDED
    return EXIT_OK
