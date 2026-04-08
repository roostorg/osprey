"""Tests for async sink infrastructure."""

import asyncio
from datetime import datetime
from typing import List
from unittest.mock import MagicMock

import pytest
from osprey.engine.executor.execution_context import Action, ExecutionResult

from osprey.async_worker.adaptor.interfaces import AsyncBaseOutputSink
from osprey.async_worker.sinks.sink.input_stream import AsyncStaticInputStream
from osprey.async_worker.sinks.sink.output_sink import AsyncMultiOutputSink, AsyncStdoutOutputSink


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
