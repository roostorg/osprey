from typing import Any, Sequence, Type

from osprey.engine.executor.execution_context import Action
from osprey.engine.udf.base import UDFBase
from osprey.worker.adaptor.plugin_manager import hookimpl_osprey
from osprey.worker.lib.config import Config
from osprey.worker.sinks.sink.input_stream import BaseInputStream
from osprey.worker.sinks.sink.output_sink import BaseOutputSink, StdoutOutputSink
from osprey.worker.sinks.utils.acking_contexts import BaseAckingContext
from plugin_sinks.kafka_input_stream import KafkaInputStream
from plugin_udfs.ban_user import BanUser
from plugin_udfs.text_contains import TextContains


@hookimpl_osprey
def register_udfs() -> Sequence[Type[UDFBase[Any, Any]]]:
    return [TextContains, BanUser]


@hookimpl_osprey
def register_input_sink(config: Config) -> BaseInputStream[BaseAckingContext[Action]]:
    return KafkaInputStream(config=config)


@hookimpl_osprey
def register_output_sinks(config: Config) -> Sequence[BaseOutputSink]:
    return [StdoutOutputSink(log_sampler=None)]
