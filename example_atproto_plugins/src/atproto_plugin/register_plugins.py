from atproto_plugin.jetstream_input_stream import JetStreamInputStream
from osprey.engine.executor.execution_context import Action
from osprey.worker.adaptor.plugin_manager import hookimpl_osprey
from osprey.worker.lib.config import Config
from osprey.worker.sinks.sink.input_stream import BaseInputStream
from osprey.worker.sinks.utils.acking_contexts import BaseAckingContext


@hookimpl_osprey
def register_input_stream(config: Config) -> BaseInputStream[BaseAckingContext[Action]]:
    endpoint = config.get_optional_str('OSPREY_JETSTREAM_ENDPOINT')
    raw_collections = config.get_optional_str('OSPREY_JETSTREAM_WANTED_COLLECTIONS')
    wanted = [c.strip() for c in raw_collections.split(',') if c.strip()] if raw_collections else None
    return JetStreamInputStream(endpoint=endpoint, wanted_collections=wanted)
