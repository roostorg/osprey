"""Plugin manager for the async worker.

Discovers plugins via the 'osprey_async_plugin' setuptools entry_point group.
All UDFs with I/O must have native async implementations — no sync fallbacks.
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


# Stdlib UDFs that have async replacements in discord_osprey_async_plugins.
# Each entry: (sync module path, sync class name, async module path, async class name)
@lru_cache(maxsize=1)
def load_all_async_plugins() -> None:
    """Load all plugins registered under the 'osprey_async_plugin' entry_point group."""
    plugin_manager.load_setuptools_entrypoints(OSPREY_ASYNC_ADAPTOR)
    plugin_manager.check_pending()


def _deduplicate_udfs(
    stdlib_udfs: List[Type[UDFBase[Any, Any]]],
    plugin_udfs: List[Type[UDFBase[Any, Any]]],
) -> List[Type[UDFBase[Any, Any]]]:
    """Merge stdlib and plugin UDFs, with plugin UDFs winning on name conflicts.

    Async plugin UDFs shadow their sync stdlib counterparts by class name.
    This lets async plugins register e.g. `HasLabel` or `MXLookup` without
    needing a separate replacement table — the plugin version just wins.
    """
    plugin_names = {udf.__name__ for udf in plugin_udfs}
    deduplicated = [udf for udf in stdlib_udfs if udf.__name__ not in plugin_names]
    deduplicated.extend(plugin_udfs)
    return deduplicated


def bootstrap_async_udfs(config: 'Config | None' = None) -> tuple[UDFRegistry, UDFHelpers]:
    """Bootstrap UDFs from async plugins + stdlib.

    Loads stdlib UDFs (JsonData, StringLength, Rule, etc.) and async plugin UDFs.
    Plugin UDFs override stdlib UDFs with the same name — this is how async
    replacements (HasLabel, MXLookup, etc.) shadow their sync counterparts.
    No sync fallbacks — all I/O UDFs must be native async.
    """
    from osprey.async_worker.stdlib_udfs.async_mx_lookup import MXLookup as AsyncMXLookup
    from osprey.engine.stdlib.udfs.mx_lookup import MXLookup as SyncMXLookup
    from osprey.worker._stdlibplugin.udf_register import register_udfs as stdlib_register_udfs

    load_all_async_plugins()
    udf_helpers = UDFHelpers()

    # Load stdlib UDFs, replacing sync MXLookup with async version
    stdlib_udfs = [u for u in stdlib_register_udfs() if u is not SyncMXLookup] + [AsyncMXLookup]
    plugin_udfs = _flatten(plugin_manager.hook.register_udfs())
    all_udfs = _deduplicate_udfs(stdlib_udfs, plugin_udfs)

    # Auto-register helpers for UDFs that extend HasHelper
    for udf in all_udfs:
        if issubclass(udf, HasHelper):
            udf_helpers.set_udf_helper(udf, udf.create_provider())

    # Wire up labels service helper for AsyncHasLabel
    labels_hook_result = _get_labels_hook_result(config)
    if labels_hook_result:
        from discord_smite.osprey_async_plugins.discord_osprey_async_plugins.udfs.async_has_label import (
            HasLabel as AsyncHasLabel,
        )

        udf_helpers.set_udf_helper(AsyncHasLabel, labels_hook_result)

    udf_registry = UDFRegistry.with_udfs(*all_udfs)
    return udf_registry, udf_helpers


def _get_labels_hook_result(config: 'Config | None') -> Any:
    """Call the labels service/provider hook. Returns the raw result or None on failure."""
    if config is None:
        return None
    if not hasattr(plugin_manager.hook, 'register_labels_service_or_provider'):
        return None
    try:
        return plugin_manager.hook.register_labels_service_or_provider(config=config)
    except Exception:
        logging.exception('Failed to register labels service/provider')
        return None


def bootstrap_async_action_proto_deserializer() -> ActionProtoDeserializer | None:
    """Bootstrap action proto deserializer from async plugins."""
    load_all_async_plugins()
    try:
        [deserializer] = plugin_manager.hook.register_action_proto_deserializer()
        return deserializer
    except Exception:
        return None


def bootstrap_validation_exporter(config: Config) -> Any:
    """Bootstrap validation result exporter from async plugins.

    Returns the exporter or None if not registered.
    """
    load_all_async_plugins()
    if not hasattr(plugin_manager.hook, 'register_validation_exporter'):
        return None
    try:
        return plugin_manager.hook.register_validation_exporter(config=config)
    except Exception:
        logging.exception('Failed to bootstrap validation exporter')
        return None


def bootstrap_async_output_sinks(config: Config) -> AsyncMultiOutputSink:
    """Bootstrap async output sinks from async plugins only.

    Does NOT load sync output sinks — the async worker uses only async sinks.
    """
    load_all_async_plugins()
    sinks: List[AsyncBaseOutputSink] = _flatten(plugin_manager.hook.register_async_output_sinks(config=config))
    return AsyncMultiOutputSink(sinks)


def bootstrap_async_ast_validators() -> None:
    """Bootstrap AST validators from async plugins + stdlib."""
    from osprey.worker._stdlibplugin.validator_regsiter import register_ast_validators as stdlib_register_validators

    load_all_async_plugins()
    validators = list(stdlib_register_validators()) + _flatten(plugin_manager.hook.register_ast_validators())

    registry = ValidatorRegistry.get_instance()
    seen = set()
    for validator in validators:
        if validator not in seen:
            seen.add(validator)
            registry.register_to_instance(validator)
