"""Example plugin registrations for the experimental asyncio worker.

This is the async counterpart to ``register_plugins`` (which targets the sync
gevent worker). It is discovered via the ``osprey_async_plugin`` entry-point
group and loaded by the async worker's plugin manager, so it never runs in the
sync worker.

Registers a pure-computation UDF (``TextContains`` runs inline in the async
executor — no I/O, so it needs no async variant) and an example async output
sink. UDFs that perform I/O must subclass ``AsyncUDFBase`` instead; see
``osprey.async_worker.stdlib_udfs.async_mx_lookup`` for that pattern.
"""

from typing import Any, Sequence, Type

from async_sinks.example_async_output_sink import ExampleAsyncOutputSink
from osprey.async_worker.adaptor.interfaces import AsyncBaseOutputSink
from osprey.async_worker.adaptor.plugin_manager import hookimpl_osprey_async
from osprey.engine.udf.base import UDFBase
from osprey.worker.lib.config import Config
from udfs.text_contains import TextContains


@hookimpl_osprey_async
def register_udfs() -> Sequence[Type[UDFBase[Any, Any]]]:
    return [TextContains]


@hookimpl_osprey_async
def register_async_output_sinks(config: Config) -> Sequence[AsyncBaseOutputSink]:
    return [ExampleAsyncOutputSink()]
