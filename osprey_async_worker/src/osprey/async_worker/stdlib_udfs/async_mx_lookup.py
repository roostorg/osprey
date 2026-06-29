"""Native async MXLookup UDF using aiodns.

Replaces the sync MXLookup which uses blocking dns.resolver calls.
Uses aiodns (c-ares) for fully async DNS resolution on the event loop
without consuming thread pool threads.

Class named `MXLookup` to shadow the sync version in the UDF registry.
"""

import asyncio

import aiodns
import pycares
from osprey.async_worker.adaptor.interfaces import AsyncUDFBase
from osprey.engine.executor.execution_context import ExecutionContext, ExpectedUdfException
from osprey.engine.stdlib.udfs.mx_lookup import Arguments
from osprey.engine.stdlib.udfs.mx_lookup import MXLookup as SyncMXLookup

_DNS_TIMEOUT = 5.0
_resolver: aiodns.DNSResolver | None = None


def _get_resolver() -> aiodns.DNSResolver:
    """Lazily create the resolver on the running event loop."""
    global _resolver
    loop = asyncio.get_running_loop()
    if _resolver is None or _resolver.loop is not loop:
        _resolver = aiodns.DNSResolver(timeout=_DNS_TIMEOUT, loop=loop)
    return _resolver


class MXLookup(AsyncUDFBase[Arguments, str]):  # type: ignore[misc]
    """Async MXLookup — uses aiodns for non-blocking DNS resolution."""

    category = SyncMXLookup.category

    @classmethod
    def _get_udf_base_args(cls):
        return (Arguments, str)

    async def async_execute(self, execution_context: ExecutionContext, arguments: Arguments) -> str:
        resolver = _get_resolver()
        try:
            mx_result = await resolver.query_dns(arguments.domain, 'MX')
            mx_records = [r for r in mx_result.answer if hasattr(r.data, 'priority')]
            if not mx_records:
                raise ExpectedUdfException()
            # hasattr filter above guarantees these are MX records; pycares' record
            # union doesn't narrow on hasattr, so suppress union-attr here.
            best_mx = sorted(mx_records, key=lambda r: r.data.priority)[0].data.exchange  # type: ignore[union-attr]
            a_result = await resolver.query_dns(best_mx, 'A')
        except (aiodns.error.DNSError, pycares.AresError):
            raise ExpectedUdfException()

        a_records = [r for r in a_result.answer if hasattr(r.data, 'addr')]
        if not a_records:
            raise ExpectedUdfException()
        # hasattr filter guarantees A records; pycares union doesn't narrow on hasattr.
        return min(r.data.addr for r in a_records)  # type: ignore[union-attr]
