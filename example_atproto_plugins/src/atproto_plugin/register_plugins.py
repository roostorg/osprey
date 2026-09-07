from collections.abc import Sequence
from typing import Any, Type

from osprey.engine.executor.execution_context import Action
from osprey.engine.udf.base import UDFBase
from osprey.worker.adaptor.plugin_manager import hookimpl_osprey
from osprey.worker.lib.config import Config
from osprey.worker.sinks.sink.input_stream import BaseInputStream
from osprey.worker.sinks.utils.acking_contexts import BaseAckingContext

from atproto_plugin.enrichment_udfs import AtprotoDisplayName, AtprotoHandle
from atproto_plugin.jetstream_input_stream import JetStreamInputStream


@hookimpl_osprey
def register_input_stream(config: Config) -> BaseInputStream[BaseAckingContext[Action]]:
    endpoint = config.get_optional_str('OSPREY_JETSTREAM_ENDPOINT')
    raw_collections = config.get_optional_str('OSPREY_JETSTREAM_WANTED_COLLECTIONS')
    wanted = [c.strip() for c in raw_collections.split(',') if c.strip()] if raw_collections else None
    return JetStreamInputStream(endpoint=endpoint, wanted_collections=wanted)


@hookimpl_osprey
def register_udfs() -> Sequence[Type[UDFBase[Any, Any]]]:
    return [AtprotoHandle, AtprotoDisplayName]
