"""Plugin manager for the async worker.

Discovers plugins via the 'osprey_async_plugin' setuptools entry_point group.
Also loads sync plugins from 'osprey_plugin' for UDFs (wrapped in adapters).
"""

from __future__ import annotations

import logging
from functools import lru_cache
from typing import TYPE_CHECKING, Any, List, Sequence, Type

import pluggy
from osprey.engine.ast_validator import ValidatorRegistry
from osprey.engine.executor.udf_execution_helpers import HasHelper, UDFHelpers
from osprey.engine.udf.base import UDFBase
from osprey.engine.udf.registry import UDFRegistry
from osprey.worker.lib.action_proto_deserializer import ActionProtoDeserializer
from osprey.worker.lib.storage.labels import LabelsProvider, LabelsServiceBase

from osprey.async_worker.adaptor import hookspecs as async_hookspecs
from osprey.async_worker.adaptor.constants import OSPREY_ASYNC_ADAPTOR
from osprey.async_worker.adaptor.interfaces import AsyncBaseOutputSink, AsyncMultiOutputSink

if TYPE_CHECKING:
    from osprey.worker.lib.config import Config

hookimpl_osprey_async: pluggy.HookimplMarker = pluggy.HookimplMarker(OSPREY_ASYNC_ADAPTOR)

plugin_manager = pluggy.PluginManager(OSPREY_ASYNC_ADAPTOR)
plugin_manager.add_hookspecs(async_hookspecs)


def _flatten(seq: List[List[Any]]) -> List[Any]:
    return sum(seq, [])


@lru_cache(maxsize=1)
def load_all_async_plugins() -> None:
    """Load all plugins registered under the 'osprey_async_plugin' entry_point group."""
    plugin_manager.load_setuptools_entrypoints(OSPREY_ASYNC_ADAPTOR)
    plugin_manager.check_pending()


def bootstrap_async_udfs(config: 'Config | None' = None, load_sync_plugins: bool = False) -> tuple[UDFRegistry, UDFHelpers]:
    """Bootstrap UDFs from async plugins + stdlib.

    Always loads stdlib UDFs (JsonData, StringLength, Rule, etc.) since
    they're needed for basic rule compilation. Optionally loads from
    osprey_plugin too (triggers gevent side effects).
    """
    # Always load stdlib UDFs — they don't trigger gevent side effects
    from osprey.worker._stdlibplugin.udf_register import register_udfs as stdlib_register_udfs

    load_all_async_plugins()
    udf_helpers = UDFHelpers()

    # Load stdlib + async plugin UDFs
    all_udfs: List[Type[UDFBase[Any, Any]]] = list(stdlib_register_udfs()) + _flatten(plugin_manager.hook.register_udfs())

    if load_sync_plugins:
        from osprey.worker.adaptor.plugin_manager import load_all_osprey_plugins, plugin_manager as sync_plugin_manager

        load_all_osprey_plugins()
        sync_udfs: List[Type[UDFBase[Any, Any]]] = _flatten(sync_plugin_manager.hook.register_udfs())

        seen = {udf for udf in all_udfs}
        for udf in sync_udfs:
            if udf not in seen:
                seen.add(udf)
                all_udfs.append(udf)

    for udf in all_udfs:
        if issubclass(udf, HasHelper):
            try:
                udf_helpers.set_udf_helper(udf, udf.create_provider())
            except Exception:
                # Skip helper creation for UDFs that fail (e.g., etcd not available).
                # These UDFs will fail at execution time via the legacy fallback,
                # which is expected — errors get captured in error_infos.
                pass

    # Bootstrap labels service (needed for HasLabel, LabelAdd, LabelRemove)
    labels_service = _bootstrap_labels_service(config)
    if labels_service:
        from osprey.engine.stdlib.udfs.labels import LabelAdd, LabelRemove

        # Use AsyncHasLabel (native async) instead of sync HasLabel.
        # AsyncHasLabel calls labels_service.async_read_labels() directly on
        # the event loop — no thread pool, no run_coroutine_threadsafe.
        try:
            from discord_smite.osprey_async_plugins.discord_osprey_async_plugins.udfs.async_has_label import (
                HasLabel as AsyncHasLabel,
            )
            from osprey.engine.stdlib.udfs.labels import HasLabel as SyncHasLabel

            # Remove sync HasLabel (registered by stdlib) — replaced by AsyncHasLabel
            all_udfs = [u for u in all_udfs if u is not SyncHasLabel]
            all_udfs.extend([AsyncHasLabel, LabelAdd, LabelRemove])
            udf_helpers.set_udf_helper(AsyncHasLabel, labels_service)
        except ImportError:
            # Fall back to sync HasLabel with LabelsProvider wrapper
            from osprey.engine.stdlib.udfs.labels import HasLabel

            labels_provider = _bootstrap_labels_provider(config)
            if labels_provider:
                all_udfs.extend([HasLabel, LabelAdd, LabelRemove])
                udf_helpers.set_udf_helper(HasLabel, labels_provider)

    udf_registry = UDFRegistry.with_udfs(*all_udfs)
    return udf_registry, udf_helpers


def _bootstrap_labels_service(config: 'Config | None') -> Any:
    """Bootstrap the raw labels service from async plugins (for AsyncHasLabel)."""
    if config is None:
        return None
    if not hasattr(plugin_manager.hook, 'register_labels_service_or_provider'):
        return None
    try:
        return plugin_manager.hook.register_labels_service_or_provider(config=config)
    except Exception:
        logging.exception('Failed to register labels service/provider')
        return None


def _bootstrap_labels_provider(config: 'Config | None') -> LabelsProvider | None:
    """Bootstrap labels provider from async plugins, following the sync worker pattern."""
    if config is None:
        return None
    if not hasattr(plugin_manager.hook, 'register_labels_service_or_provider'):
        return None
    try:
        provider_or_service = plugin_manager.hook.register_labels_service_or_provider(config=config)
    except Exception:
        logging.exception('Failed to register labels service/provider')
        return None
    if provider_or_service is None:
        return None
    if isinstance(provider_or_service, LabelsProvider):
        provider_or_service.initialize()
        return provider_or_service
    if isinstance(provider_or_service, LabelsServiceBase):
        provider = LabelsProvider(provider_or_service)
        provider.initialize()
        return provider
    return None


def bootstrap_async_action_proto_deserializer() -> ActionProtoDeserializer | None:
    """Bootstrap action proto deserializer from async plugins."""
    load_all_async_plugins()
    try:
        [deserializer] = plugin_manager.hook.register_action_proto_deserializer()
        return deserializer
    except Exception:
        return None


def bootstrap_async_output_sinks(config: Config) -> AsyncMultiOutputSink:
    """Bootstrap async output sinks from async plugins only.

    Does NOT load sync output sinks — the async worker uses only async sinks.
    """
    load_all_async_plugins()
    sinks: List[AsyncBaseOutputSink] = _flatten(plugin_manager.hook.register_async_output_sinks(config=config))
    return AsyncMultiOutputSink(sinks)


def bootstrap_async_ast_validators(load_sync_plugins: bool = False) -> None:
    """Bootstrap AST validators from async plugins + stdlib.

    Always loads stdlib validators (ValidateCallKwargs, etc.) since they're
    needed for rule compilation. Optionally loads from osprey_plugin too.
    """
    # Always load stdlib validators — they don't trigger gevent side effects
    from osprey.worker._stdlibplugin.validator_regsiter import register_ast_validators as stdlib_register_validators

    load_all_async_plugins()
    validators = list(stdlib_register_validators()) + _flatten(plugin_manager.hook.register_ast_validators())

    if load_sync_plugins:
        from osprey.worker.adaptor.plugin_manager import load_all_osprey_plugins, plugin_manager as sync_plugin_manager

        load_all_osprey_plugins()
        sync_validators = _flatten(sync_plugin_manager.hook.register_ast_validators())
        validators = validators + sync_validators

    registry = ValidatorRegistry.get_instance()
    seen = set()
    for validator in validators:
        if validator not in seen:
            seen.add(validator)
            registry.register_to_instance(validator)
