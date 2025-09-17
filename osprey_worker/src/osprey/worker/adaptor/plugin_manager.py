from __future__ import annotations

from functools import lru_cache
from typing import TYPE_CHECKING, Any, List, Type, TypeVar

import pluggy
from osprey.engine.ast_validator import ValidatorRegistry
from osprey.engine.executor.execution_context import Action
from osprey.engine.executor.udf_execution_helpers import HasHelper, UDFHelpers
from osprey.engine.udf.base import UDFBase
from osprey.engine.udf.registry import UDFRegistry
from osprey.worker.adaptor.constants import OSPREY_ADAPTOR
from osprey.worker.adaptor.hookspecs import osprey_hooks
from osprey.worker.lib.action_proto_deserializer import ActionProtoDeserializer
from osprey.worker.lib.storage.labels import HasLabelProvider
from osprey.worker.sinks import BaseInputStream, BaseAckingContext
from osprey.worker.sinks.sink.output_sink import BaseOutputSink, MultiOutputSink

if TYPE_CHECKING:
    from osprey.worker.lib.config import Config

hookimpl_osprey: pluggy.HookimplMarker = pluggy.HookimplMarker(OSPREY_ADAPTOR)

plugin_manager = pluggy.PluginManager(OSPREY_ADAPTOR)
plugin_manager.add_hookspecs(osprey_hooks)


@lru_cache(maxsize=1)
def load_all_osprey_plugins():
    plugin_manager.load_setuptools_entrypoints(OSPREY_ADAPTOR)
    plugin_manager.check_pending()


T = TypeVar('T')


def flatten(seq: List[List[T]]) -> List[T]:
    return sum(seq, [])


def bootstrap_udfs() -> tuple[UDFRegistry, UDFHelpers]:
    load_all_osprey_plugins()
    udf_helpers = UDFHelpers()

    udfs: List[Type[UDFBase[Any, Any]]] = flatten(plugin_manager.hook.register_udfs())
    udf_registry = UDFRegistry.with_udfs(*udfs)

    for udf in udfs:
        if issubclass(udf, HasHelper):
            udf_helpers.set_udf_helper(udf, udf.create_provider())

    # ayu change this one - note that referencing HasLabel has some odd circular imports so its done here
    from osprey.engine.stdlib.udfs.labels import HasLabel

    udf_helpers.set_udf_helper(HasLabel, HasLabelProvider())

    return udf_registry, udf_helpers


def bootstrap_output_sinks(config: Config) -> BaseOutputSink:
    load_all_osprey_plugins()
    sinks = flatten(plugin_manager.hook.register_output_sinks(config=config))
    return MultiOutputSink(sinks)


def bootstrap_ast_validators() -> None:
    load_all_osprey_plugins()
    validators = flatten(plugin_manager.hook.register_ast_validators())
    registry = ValidatorRegistry.get_instance()
    for validator in validators:
        registry.register_to_instance(validator)


def bootstrap_action_proto_deserializer() -> ActionProtoDeserializer | None:
    load_all_osprey_plugins()

    try:
        [deserializer] = plugin_manager.hook.register_action_proto_deserializer()
        return deserializer
    except Exception:
        return None

def bootstrap_input_stream() -> BaseInputStream[BaseAckingContext[Action]] | None:
    load_all_osprey_plugins()

    streams = plugin_manager.hook.register_input_stream()
    if streams:
        # spec has firstresult=True set, so at most it will be one.
        return streams[0]
    else:
        return None

