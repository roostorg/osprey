"""Native async MXLookup UDF.

Replaces the sync MXLookup which has execute_async=True but no async_execute method.
Uses run_in_executor to run the blocking dns.resolver calls off the event loop.

Class named `MXLookup` to shadow the sync version in the UDF registry.
"""

import asyncio

from dns.resolver import NXDOMAIN, YXDOMAIN, NoAnswer, NoNameservers
from osprey.async_worker.adaptor.interfaces import AsyncUDFBase
from osprey.engine.executor.execution_context import ExecutionContext, ExpectedUdfException
from osprey.engine.stdlib.udfs.mx_lookup import Arguments, resolver
from osprey.engine.stdlib.udfs.mx_lookup import MXLookup as SyncMXLookup


class MXLookup(AsyncUDFBase[Arguments, str]):  # type: ignore[misc]
    """Async MXLookup — runs DNS resolution in a thread pool to avoid blocking the event loop."""

    category = SyncMXLookup.category

    @classmethod
    def _get_udf_base_args(cls):
        return (Arguments, str)

    async def async_execute(self, execution_context: ExecutionContext, arguments: Arguments) -> str:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._resolve, arguments)

    @staticmethod
    def _resolve(arguments: Arguments) -> str:
        try:
            mx_answer = resolver.resolve(arguments.domain, 'MX', raise_on_no_answer=True)[0]
            domain = mx_answer.exchange.to_text()
            a_record_answers = resolver.resolve(domain, 'A', raise_on_no_answer=True)
        except (NoAnswer, NXDOMAIN, YXDOMAIN, NoNameservers):
            raise ExpectedUdfException()

        return min(answer.to_text() for answer in a_record_answers)
