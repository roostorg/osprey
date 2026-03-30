"""Async plugin interfaces for the osprey async worker.

These mirror the sync interfaces in osprey.worker but use async/await.
Sync plugins can be wrapped with the adapters below for backward compatibility.
"""

import abc
import asyncio
from typing import Any, Generic, Optional, Sequence, TypeVar

from osprey.engine.executor.execution_context import ExecutionContext, ExecutionResult
from osprey.engine.udf.base import BatchableUDFBase, UDFBase
from osprey.engine.udf.arguments import ArgumentsBase
from result import Result

Arguments = TypeVar('Arguments', bound=ArgumentsBase)
BatchableArguments = TypeVar('BatchableArguments')
RValue = TypeVar('RValue')
_T = TypeVar('_T')


class AsyncUDFBase(Generic[Arguments, RValue], abc.ABC):
    """Async version of UDFBase. Implement this for UDFs that do async I/O natively."""

    execute_async = True

    @abc.abstractmethod
    async def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> RValue:
        raise NotImplementedError


class AsyncBatchableUDFBase(Generic[Arguments, RValue, BatchableArguments], AsyncUDFBase[Arguments, RValue]):
    """Async version of BatchableUDFBase."""

    @abc.abstractmethod
    async def execute_batch(
        self,
        execution_context: ExecutionContext,
        udfs: Sequence[UDFBase[Any, Any]],
        arguments: Sequence[BatchableArguments],
    ) -> Sequence[Result[RValue, Exception]]:
        raise NotImplementedError


class AsyncBaseOutputSink(abc.ABC):
    """Async version of BaseOutputSink."""

    timeout: float = 2.0
    max_retries: int = 0

    @abc.abstractmethod
    def will_do_work(self, result: ExecutionResult) -> bool:
        """Sync check — no I/O needed, just inspects the result."""
        raise NotImplementedError

    @abc.abstractmethod
    async def push(self, result: ExecutionResult) -> None:
        raise NotImplementedError

    async def stop(self) -> None:
        pass


class AsyncBaseInputStream(abc.ABC, Generic[_T]):
    """Async version of BaseInputStream. Uses async iteration."""

    @abc.abstractmethod
    async def __aiter__(self):
        raise NotImplementedError

    @abc.abstractmethod
    async def __anext__(self) -> _T:
        raise NotImplementedError

    async def stop(self) -> None:
        pass


class AsyncMultiOutputSink(AsyncBaseOutputSink):
    """Tees execution results to multiple async output sinks."""

    def __init__(self, sinks: Sequence[AsyncBaseOutputSink]):
        self._sinks = sinks

    def will_do_work(self, result: ExecutionResult) -> bool:
        return any(sink.will_do_work(result) for sink in self._sinks)

    async def push(self, result: ExecutionResult) -> None:
        for sink in self._sinks:
            if sink.will_do_work(result):
                try:
                    async with asyncio.timeout(sink.timeout):
                        await sink.push(result)
                except TimeoutError:
                    # Log/metric, but don't fail the whole pipeline
                    pass
                except Exception:
                    pass

    async def stop(self) -> None:
        for sink in self._sinks:
            await sink.stop()


# --- Adapters: wrap sync plugins to run in async worker ---


class SyncUDFAdapter:
    """Wraps a sync UDFBase to run in a thread pool executor within the async event loop."""

    def __init__(self, sync_udf: UDFBase[Any, Any]):
        self._sync_udf = sync_udf

    async def execute(self, execution_context: ExecutionContext, arguments: Any) -> Any:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._sync_udf.execute, execution_context, arguments)


class SyncBatchableUDFAdapter:
    """Wraps a sync BatchableUDFBase to run in a thread pool executor."""

    def __init__(self, sync_udf: BatchableUDFBase[Any, Any, Any]):
        self._sync_udf = sync_udf

    async def execute_batch(
        self,
        execution_context: ExecutionContext,
        udfs: Sequence[UDFBase[Any, Any]],
        arguments: Sequence[Any],
    ) -> Sequence[Result[Any, Exception]]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, self._sync_udf.execute_batch, execution_context, udfs, arguments
        )


class SyncOutputSinkAdapter(AsyncBaseOutputSink):
    """Wraps a sync BaseOutputSink to run in a thread pool executor."""

    def __init__(self, sync_sink: Any):
        self._sync_sink = sync_sink
        self.timeout = getattr(sync_sink, 'timeout', 2.0)
        self.max_retries = getattr(sync_sink, 'max_retries', 0)

    def will_do_work(self, result: ExecutionResult) -> bool:
        return self._sync_sink.will_do_work(result)

    async def push(self, result: ExecutionResult) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._sync_sink.push, result)

    async def stop(self) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._sync_sink.stop)
