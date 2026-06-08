"""Tests for async sink infrastructure."""

import asyncio
from datetime import datetime
from typing import List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from osprey.async_worker.adaptor.interfaces import AsyncBaseOutputSink
from osprey.async_worker.sinks.sink import rules_sink as rules_sink_module
from osprey.async_worker.sinks.sink.input_stream import AsyncStaticInputStream
from osprey.async_worker.sinks.sink.output_sink import AsyncMultiOutputSink, AsyncStdoutOutputSink
from osprey.async_worker.sinks.sink.rules_sink import AsyncRulesRunner
from osprey.engine.executor.execution_context import Action, ExecutionResult


def _make_result(action_id: int = 1, action_name: str = 'test') -> ExecutionResult:
    return ExecutionResult(
        extracted_features={},
        action=Action(
            action_id=action_id,
            action_name=action_name,
            data={},
            timestamp=datetime.utcnow(),
        ),
        effects={},
        error_infos=[],
        validator_results=None,
        sample_rate=100,
    )


# --- AsyncStaticInputStream ---


@pytest.mark.asyncio
async def test_static_input_stream_yields_all_items():
    items = ['a', 'b', 'c']
    stream = AsyncStaticInputStream(items)
    collected = []
    async for item in stream:
        collected.append(item)
    assert collected == items


@pytest.mark.asyncio
async def test_static_input_stream_empty():
    stream = AsyncStaticInputStream([])
    collected = []
    async for item in stream:
        collected.append(item)
    assert collected == []


# --- AsyncMultiOutputSink ---


class RecordingSink(AsyncBaseOutputSink):
    """Test sink that records all pushed results."""

    def __init__(self):
        self.results: List[ExecutionResult] = []
        self.stopped = False

    def will_do_work(self, result: ExecutionResult) -> bool:
        return True

    async def push(self, result: ExecutionResult) -> None:
        self.results.append(result)

    async def stop(self) -> None:
        self.stopped = True


class SelectiveSink(AsyncBaseOutputSink):
    """Test sink that only processes specific action names."""

    def __init__(self, allowed: str):
        self._allowed = allowed
        self.results: List[ExecutionResult] = []

    def will_do_work(self, result: ExecutionResult) -> bool:
        return result.action.action_name == self._allowed

    async def push(self, result: ExecutionResult) -> None:
        self.results.append(result)

    async def stop(self) -> None:
        pass


class FailingSink(AsyncBaseOutputSink):
    """Test sink that always raises."""

    def will_do_work(self, result: ExecutionResult) -> bool:
        return True

    async def push(self, result: ExecutionResult) -> None:
        raise RuntimeError('sink failure')

    async def stop(self) -> None:
        pass


class SlowSink(AsyncBaseOutputSink):
    """Test sink that takes too long."""

    timeout = 0.05

    def __init__(self):
        self.attempted = False

    def will_do_work(self, result: ExecutionResult) -> bool:
        return True

    async def push(self, result: ExecutionResult) -> None:
        self.attempted = True
        await asyncio.sleep(1.0)  # way longer than timeout

    async def stop(self) -> None:
        pass


@pytest.mark.asyncio
async def test_multi_sink_pushes_to_all():
    sink_a = RecordingSink()
    sink_b = RecordingSink()
    multi = AsyncMultiOutputSink([sink_a, sink_b])

    result = _make_result()
    await multi.push(result)

    assert len(sink_a.results) == 1
    assert len(sink_b.results) == 1


@pytest.mark.asyncio
async def test_multi_sink_respects_will_do_work():
    sink_a = SelectiveSink('action_a')
    sink_b = SelectiveSink('action_b')
    multi = AsyncMultiOutputSink([sink_a, sink_b])

    await multi.push(_make_result(action_name='action_a'))
    await multi.push(_make_result(action_name='action_b'))
    await multi.push(_make_result(action_name='action_c'))

    assert len(sink_a.results) == 1
    assert len(sink_b.results) == 1


@pytest.mark.asyncio
async def test_multi_sink_continues_after_failure():
    """A failing sink doesn't prevent other sinks from receiving results."""
    failing = FailingSink()
    recording = RecordingSink()
    multi = AsyncMultiOutputSink([failing, recording])

    await multi.push(_make_result())

    # Recording sink still got the result despite failing sink
    assert len(recording.results) == 1


@pytest.mark.asyncio
async def test_multi_sink_handles_timeout():
    """A slow sink times out without blocking other sinks."""
    slow = SlowSink()
    recording = RecordingSink()
    multi = AsyncMultiOutputSink([slow, recording])

    await multi.push(_make_result())

    assert slow.attempted is True
    assert len(recording.results) == 1


@pytest.mark.asyncio
async def test_multi_sink_stop():
    sink_a = RecordingSink()
    sink_b = RecordingSink()
    multi = AsyncMultiOutputSink([sink_a, sink_b])

    await multi.stop()

    assert sink_a.stopped is True
    assert sink_b.stopped is True


@pytest.mark.asyncio
async def test_stdout_sink_will_do_work():
    sink = AsyncStdoutOutputSink()
    assert sink.will_do_work(_make_result()) is True


# --- AsyncRulesRunner dispatch wiring ---


@pytest.mark.asyncio
async def test_classify_one_routes_through_engine_execute_for_dispatch():
    """Regression: classify_one MUST execute via engine.execute() (which runs the
    typed-action-contract resolve_dispatch + shadow), NOT via async_execute() against the
    full execution_graph directly. The direct-graph path made the specializer a runtime
    no-op on the asyncio worker (the sole prod path) — specialized graphs registered at
    init but never served. This pins the sink to the dispatch-aware engine method."""
    served = _make_result(action_id=123, action_name='guild_invite_created')
    engine = MagicMock()
    engine.execute = AsyncMock(return_value=served)
    # No per-action sample config -> ActionSampler returns _SAMPLE_NEVER (action not dropped).
    engine.get_config_subkey.return_value.get_action_config.return_value = None

    output_sink = MagicMock()
    output_sink.push = AsyncMock()

    runner = AsyncRulesRunner(engine, output_sink, MagicMock(), max_concurrent_udfs=1)
    action = Action(action_id=123, action_name='guild_invite_created', data={}, timestamp=datetime.utcnow())

    with patch.object(rules_sink_module, 'metrics', MagicMock()):
        result = await runner.classify_one(action, tag='test')

    # The dispatch-aware engine method was used (the bug bypassed it for a direct full-graph run).
    engine.execute.assert_awaited_once()
    assert action in engine.execute.call_args.args, 'engine.execute must receive the action'
    # The served result flows to the output sink and is returned.
    output_sink.push.assert_awaited_once_with(served)
    assert result is served
