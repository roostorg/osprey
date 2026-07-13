"""Reference async output sink for the experimental asyncio worker.

Demonstrates the ``register_async_output_sinks`` hook: the async worker awaits
``push(result)`` for each execution result. Output sinks for the async worker
must subclass ``AsyncBaseOutputSink`` (not the sync ``BaseOutputSink``), since
the async worker runs on asyncio without gevent.
"""

import logging

from osprey.async_worker.adaptor.interfaces import AsyncBaseOutputSink
from osprey.engine.executor.execution_context import ExecutionResult

logger = logging.getLogger(__name__)


class ExampleAsyncOutputSink(AsyncBaseOutputSink):
    """Logs each result's extracted features and verdicts."""

    def will_do_work(self, result: ExecutionResult) -> bool:
        return True

    async def push(self, result: ExecutionResult) -> None:
        logger.info(
            'example async output sink: features=%s verdicts=%s',
            result.extracted_features_json,
            result.verdicts,
        )

    async def stop(self) -> None:
        pass
