from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence, Type

import pluggy
from osprey.engine.ast_validator.base_validator import BaseValidator
from osprey.engine.executor.execution_context import Action
from osprey.engine.udf.base import UDFBase
from osprey.worker.adaptor.constants import OSPREY_ADAPTOR
from osprey.worker.lib.action_proto_deserializer import ActionProtoDeserializer
from osprey.worker.lib.storage.labels import LabelProvider
from osprey.worker.sinks.sink.input_stream import BaseInputStream
from osprey.worker.sinks.utils.acking_contexts import BaseAckingContext

if TYPE_CHECKING:
    from osprey.worker.lib.config import Config
    from osprey.worker.lib.storage.stored_execution_result import ExecutionResultStore
    from osprey.worker.sinks.sink.output_sink import BaseOutputSink

hookspec: pluggy.HookspecMarker = pluggy.HookspecMarker(OSPREY_ADAPTOR)


@hookspec
def register_udfs() -> Sequence[Type[UDFBase[Any, Any]]]:
    """Register a new UDF to the UDFRegistry."""
    raise NotImplementedError('register_udfs must be implemented by the plugin')


@hookspec
def register_output_sinks(config: Config) -> Sequence[BaseOutputSink]:
    """Returns output sink instances to use with the engine."""
    raise NotImplementedError('register_output_sinks must be implemented by the plugin')


@hookspec
def register_ast_validators() -> Sequence[Type[BaseValidator]]:
    """Register a new AST validator."""
    raise NotImplementedError('register_ast_validators must be implemented by the plugin')


@hookspec
def register_action_proto_deserializer() -> ActionProtoDeserializer | None:
    """Register a custom deserializer to convert custom Action proto into JSON."""
    raise NotImplementedError('register_action_proto_deserializers must be implemented by the plugin')


@hookspec(firstresult=True)
def register_input_stream(config: Config) -> BaseInputStream[BaseAckingContext[Action]]:
    raise NotImplementedError('register_input_stream must be implemented by the plugin')


@hookspec(firstresult=True)
def register_execution_result_store(config: Config) -> ExecutionResultStore:
    """Register an execution result storage backend instance."""
    raise NotImplementedError('register_execution_result_store must be implemented by the plugin')


@hookspec(firstresult=True)
def register_label_provider(config: Config) -> LabelProvider:
    """Register an execution result storage backend instance."""
    raise NotImplementedError('register_label_provider must be implemented by the plugin')
