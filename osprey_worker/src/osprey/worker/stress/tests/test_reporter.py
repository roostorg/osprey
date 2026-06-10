import json

import pytest
from osprey.worker.stress.reporter import (
    EXIT_DROP_RATE_EXCEEDED,
    EXIT_LATENCY_EXCEEDED,
    EXIT_OK,
    Thresholds,
    compute_latency_stats,
    compute_report,
    exit_code_for,
    percentile,
)


class TestPercentile:
    def test_raises_on_empty(self) -> None:
        with pytest.raises(ValueError, match='empty'):
            percentile([], 50)

    def test_raises_out_of_range(self) -> None:
        with pytest.raises(ValueError):
            percentile([1.0], -1)
        with pytest.raises(ValueError):
            percentile([1.0], 101)

    def test_single_value(self) -> None:
        assert percentile([5.0], 0) == 5.0
        assert percentile([5.0], 50) == 5.0
        assert percentile([5.0], 100) == 5.0

    def test_nearest_rank_known_values(self) -> None:
        # 1..10 sorted; nearest-rank p95 = ceil(0.95*10)-1 = 9, sorted[9] = 10
        vs = [float(i) for i in range(1, 11)]
        assert percentile(vs, 50) == 5.0  # ceil(5)-1 = 4 → sorted[4] = 5
        assert percentile(vs, 95) == 10.0
        assert percentile(vs, 99) == 10.0
        assert percentile(vs, 0) == 1.0


class TestComputeLatencyStats:
    def test_all_fields_populated(self) -> None:
        stats = compute_latency_stats([100.0, 50.0, 25.0, 75.0, 200.0])
        assert stats.min_ms == 25.0
        assert stats.max_ms == 200.0
        assert stats.count == 5
        # Sorted: 25, 50, 75, 100, 200; p50 = sorted[ceil(2.5)-1] = sorted[2] = 75
        assert stats.p50_ms == 75.0

    def test_raises_on_empty(self) -> None:
        # Match percentile()'s ValueError instead of letting an IndexError leak;
        # this function is public and callers deserve a clear error.
        with pytest.raises(ValueError, match='non-empty'):
            compute_latency_stats([])


class TestComputeReport:
    def test_closed_loop_perfect_delivery(self) -> None:
        produced = {f'id-{i}': float(i) for i in range(10)}
        consumed = {f'id-{i}': float(i) + 0.1 for i in range(10)}
        r = compute_report(produced=produced, consumed=consumed, duration_seconds=1.0)
        assert r.mode == 'closed-loop'
        assert r.produced == 10
        assert r.consumed == 10
        assert r.matched == 10
        assert r.drop_count == 0
        assert r.drop_rate == 0.0
        assert r.latency is not None
        assert r.latency.count == 10
        # latency ≈ 100ms across the board
        assert 99.0 < r.latency.p50_ms < 101.0
        assert r.threshold_breaches == ()

    def test_closed_loop_partial_drop(self) -> None:
        produced = {f'id-{i}': 0.0 for i in range(10)}
        consumed = {f'id-{i}': 0.1 for i in range(7)}  # 3 dropped
        r = compute_report(produced=produced, consumed=consumed, duration_seconds=1.0)
        assert r.drop_count == 3
        assert r.drop_rate == 0.3
        assert r.matched == 7

    def test_closed_loop_consumed_includes_unknown_ids(self) -> None:
        # Consumer saw some IDs we never sent (e.g. other test traffic).
        # Those should not count as matched but should appear in consumed total.
        produced = {'id-1': 0.0, 'id-2': 0.0}
        consumed = {'id-1': 0.1, 'id-2': 0.1, 'other-id': 0.1}
        r = compute_report(produced=produced, consumed=consumed, duration_seconds=1.0)
        assert r.produced == 2
        assert r.consumed == 3
        assert r.matched == 2
        assert r.drop_count == 0

    def test_open_loop_no_produced(self) -> None:
        # No produce timestamps → can't compute drop or latency, just throughput.
        consumed = {f'id-{i}': 0.1 for i in range(50)}
        r = compute_report(produced={}, consumed=consumed, duration_seconds=10.0)
        assert r.mode == 'open-loop'
        assert r.produced == 0
        assert r.consumed == 50
        assert r.matched == 50
        assert r.drop_count == 0
        assert r.drop_rate == 0.0
        assert r.latency is None

    def test_drop_rate_breach_recorded(self) -> None:
        produced = {f'id-{i}': 0.0 for i in range(10)}
        consumed = {f'id-{i}': 0.1 for i in range(5)}
        r = compute_report(
            produced=produced,
            consumed=consumed,
            duration_seconds=1.0,
            thresholds=Thresholds(drop_rate=0.1),
        )
        assert any('drop_rate' in b for b in r.threshold_breaches)
        assert exit_code_for(r) == EXIT_DROP_RATE_EXCEEDED

    def test_p95_breach_recorded(self) -> None:
        # p95 will be 500ms, threshold is 100ms → breach.
        produced = {f'id-{i}': 0.0 for i in range(10)}
        consumed = {f'id-{i}': 0.5 for i in range(10)}
        r = compute_report(
            produced=produced,
            consumed=consumed,
            duration_seconds=1.0,
            thresholds=Thresholds(p95_ms=100.0),
        )
        assert any('p95_ms' in b for b in r.threshold_breaches)
        assert exit_code_for(r) == EXIT_LATENCY_EXCEEDED

    def test_no_breach_returns_ok(self) -> None:
        produced = {f'id-{i}': 0.0 for i in range(10)}
        consumed = {f'id-{i}': 0.05 for i in range(10)}
        r = compute_report(
            produced=produced,
            consumed=consumed,
            duration_seconds=1.0,
            thresholds=Thresholds(drop_rate=0.01, p95_ms=100.0),
        )
        assert r.threshold_breaches == ()
        assert exit_code_for(r) == EXIT_OK

    def test_empty_produced_and_consumed(self) -> None:
        r = compute_report(produced={}, consumed={}, duration_seconds=1.0)
        assert r.mode == 'open-loop'
        assert r.drop_rate == 0.0
        assert r.latency is None

    def test_json_roundtrip(self) -> None:
        produced = {'id-1': 0.0}
        consumed = {'id-1': 0.1}
        r = compute_report(produced=produced, consumed=consumed, duration_seconds=1.0)
        payload = json.loads(r.to_json())
        assert payload['mode'] == 'closed-loop'
        assert payload['matched'] == 1
        assert 'latency' in payload

    def test_human_format_contains_key_fields(self) -> None:
        produced = {'id-1': 0.0}
        consumed = {'id-1': 0.1}
        r = compute_report(produced=produced, consumed=consumed, duration_seconds=1.0)
        out = r.to_human()
        assert 'mode:' in out
        assert 'drop rate:' in out
        assert 'latency' in out
