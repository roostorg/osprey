"""Async output sink with timeout and retry support."""

import asyncio
import logging
from typing import Sequence

from osprey.engine.executor.execution_context import ExecutionResult
from osprey.worker.lib.instruments import metrics

from osprey.async_worker.adaptor.interfaces import AsyncBaseOutputSink

logger = logging.getLogger(__name__)


class AsyncMultiOutputSink(AsyncBaseOutputSink):
    """Tees execution results to multiple async output sinks with timeout and retry."""

    def __init__(self, sinks: Sequence[AsyncBaseOutputSink]):
        self._sinks = sinks

    def will_do_work(self, result: ExecutionResult) -> bool:
        return any(sink.will_do_work(result) for sink in self._sinks)

    async def push(self, result: ExecutionResult) -> None:
        tasks = []
        for sink in self._sinks:
            if sink.will_do_work(result):
                tasks.append(self._push_one(sink, result))
        if tasks:
            await asyncio.gather(*tasks)

    async def _push_one(self, sink: AsyncBaseOutputSink, result: ExecutionResult) -> None:
        """Push to a single sink with timeout and retry. Runs concurrently via gather()."""
        sink_name = sink.__class__.__name__
        attempts = sink.max_retries + 1

        for attempt in range(1, attempts + 1):
            try:
                start = asyncio.get_running_loop().time()
                async with asyncio.timeout(sink.timeout):
                    await sink.push(result)
                metrics.timing('handled_message_output', (asyncio.get_running_loop().time() - start) * 1000, tags=[f'sink:{sink_name}'])
                break
            except TimeoutError:
                logger.warning(f'Timeout pushing to {sink_name} (attempt {attempt}/{attempts})')
                metrics.increment('output_sink.timeout', tags=[f'sink:{sink_name}'])
                if attempt == attempts:
                    metrics.increment('output_sink.timeout_exhausted', tags=[f'sink:{sink_name}'])
            except Exception as exc:
                logger.exception(f'Error pushing to {sink_name}: {exc}')
                metrics.increment(
                    'output_sink.error', tags=[f'sink:{sink_name}', f'error:{exc.__class__.__name__}']
                )
                if attempt == attempts:
                    break
                await asyncio.sleep(0.5 * attempt)

    async def stop(self) -> None:
        for sink in self._sinks:
            await sink.stop()


class AsyncStdoutOutputSink(AsyncBaseOutputSink):
    """Debug output sink that prints to stdout."""

    def will_do_work(self, result: ExecutionResult) -> bool:
        return True

    async def push(self, result: ExecutionResult) -> None:
        logger.info(f'result: {result.extracted_features_json} {result.verdicts}')

    async def stop(self) -> None:
        pass
