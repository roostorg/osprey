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
from osprey.worker.lib.singletons import LABELS_PROVIDER
from osprey.worker.lib.storage.labels import LabelsProvider, LabelsServiceBase
from osprey.worker.sinks.sink.input_stream import BaseInputStream
from osprey.worker.sinks.sink.output_sink import BaseOutputSink, LabelOutputSink, MultiOutputSink
from osprey.worker.sinks.utils.acking_contexts import BaseAckingContext

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


def _labels_service_or_provider_is_registered() -> bool:
    return hasattr(plugin_manager.hook, 'register_labels_service_or_provider')


def bootstrap_udfs() -> tuple[UDFRegistry, UDFHelpers]:
    load_all_osprey_plugins()
    udf_helpers = UDFHelpers()

    udfs: List[Type[UDFBase[Any, Any]]] = flatten(plugin_manager.hook.register_udfs())

    for udf in udfs:
        if issubclass(udf, HasHelper):
            udf_helpers.set_udf_helper(udf, udf.create_provider())

    # Label udfs should only be registered if the labels provider is available
    # from osprey.worker.lib.singletons import LABELS_PROVIDER

    labels_provider = LABELS_PROVIDER.instance()
    if labels_provider:
        # Imports kinda circular. Imports here are to avoid that.
        from osprey.engine.stdlib.udfs.labels import HasLabel, LabelAdd, LabelRemove

        udfs.extend([HasLabel, LabelAdd, LabelRemove])

        udf_helpers.set_udf_helper(HasLabel, labels_provider)

    udf_registry = UDFRegistry.with_udfs(*udfs)

    return udf_registry, udf_helpers


def bootstrap_output_sinks(config: Config) -> BaseOutputSink:
    load_all_osprey_plugins()
    sinks = flatten(plugin_manager.hook.register_output_sinks(config=config))

    # Label udfs should only be registered if the labels provider is available
    # from osprey.worker.lib.singletons import LABELS_PROVIDER

    labels_provider = LABELS_PROVIDER.instance()
    if labels_provider:
        sinks.append(LabelOutputSink(labels_provider))

    return MultiOutputSink(sinks)


def bootstrap_labels_provider(config: Config) -> LabelsProvider:
    """
    NOTE: If you are looking to get a labels provider to use within Osprey,
    it is best practice to reference the LABELS_PROVIDER singleton in
    `osprey_worker/src/osprey/worker/lib/singletons.py` by calling
    `LABELS_PROVIDER.instance()`

    This way, any statefulness that implementers want for `LabelsServiceBase`
    or `LabelsProvider` will be respected across a given Osprey worker.

    -

    Generates a bootstrapped labels provider using the registered plugin.
    This will also call `initialize()` on the LabelsProvider, which will call
    `initialize()` on the LabelsServiceBase by default.

    This will throw an assertion error if `register_labels_service_or_provider`
    does not exist, i.e. in the event that a labels service / provider was not
    configured.
    """
    load_all_osprey_plugins()
    if not _labels_service_or_provider_is_registered():
        raise NotImplementedError(
            'bootstrap_labels_provider() assumes `register_labels_service_or_provider` is implemented.'
        )
    provider_or_service = plugin_manager.hook.register_labels_service_or_provider(config=config)
    assert isinstance(provider_or_service, LabelsProvider) or isinstance(provider_or_service, LabelsServiceBase), (
        f"invariant: `register_labels_service_or_provider` has an invalid return type: '{type(provider_or_service)}';",
        "expected 'LabelsServiceBase' or 'LabelsProvider'",
    )
    if isinstance(provider_or_service, LabelsServiceBase):
        provider = LabelsProvider(provider_or_service)
        provider.initialize()
        return provider
    # if we reach here, then a provider was supplied by the hook !
    provider_or_service.initialize()
    return provider_or_service


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


def bootstrap_input_stream(config: Config) -> BaseInputStream[BaseAckingContext[Action]] | None:
    load_all_osprey_plugins()

    # spec has firstresult=True set, so it will return the first registered stream if one is registered
    stream = plugin_manager.hook.register_input_stream(config=config)
    if stream:
        return stream
    else:
        return None


def bootstrap_execution_result_store(config: Config):
    """Get the execution result storage backend from plugins."""
    load_all_osprey_plugins()

    try:
        store = plugin_manager.hook.register_execution_result_store(config=config)
        return store
    except Exception:
        return None
