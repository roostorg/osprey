"""Plugin manager for the async worker.

Discovers plugins via the 'osprey_async_plugin' setuptools entry_point group.
Also loads sync plugins from 'osprey_plugin' for UDFs (wrapped in adapters).
"""

from __future__ import annotations

from functools import lru_cache
from typing import TYPE_CHECKING, Any, List, Sequence, Type

import pluggy
from osprey.engine.ast_validator import ValidatorRegistry
from osprey.engine.executor.udf_execution_helpers import HasHelper, UDFHelpers
from osprey.engine.udf.base import UDFBase
from osprey.engine.udf.registry import UDFRegistry

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


def bootstrap_async_udfs() -> tuple[UDFRegistry, UDFHelpers]:
    """Bootstrap UDFs from async plugins.

    Also loads sync plugins' UDFs via the existing osprey_plugin system,
    since UDFs are the same regardless of worker type.
    """
    load_all_async_plugins()
    udf_helpers = UDFHelpers()

    # Load UDFs from async plugins
    async_udfs: List[Type[UDFBase[Any, Any]]] = _flatten(plugin_manager.hook.register_udfs())

    # Also load UDFs from sync plugins (they work in the async executor via run_in_executor)
    from osprey.worker.adaptor.plugin_manager import load_all_osprey_plugins, plugin_manager as sync_plugin_manager

    load_all_osprey_plugins()
    sync_udfs: List[Type[UDFBase[Any, Any]]] = _flatten(sync_plugin_manager.hook.register_udfs())

    # Merge, deduplicating by class
    seen = set()
    all_udfs = []
    for udf in async_udfs + sync_udfs:
        if udf not in seen:
            seen.add(udf)
            all_udfs.append(udf)

    for udf in all_udfs:
        if issubclass(udf, HasHelper):
            udf_helpers.set_udf_helper(udf, udf.create_provider())

    udf_registry = UDFRegistry.with_udfs(*all_udfs)
    return udf_registry, udf_helpers


def bootstrap_async_output_sinks(config: Config) -> AsyncMultiOutputSink:
    """Bootstrap async output sinks from async plugins only.

    Does NOT load sync output sinks — the async worker uses only async sinks.
    """
    load_all_async_plugins()
    sinks: List[AsyncBaseOutputSink] = _flatten(plugin_manager.hook.register_async_output_sinks(config=config))
    return AsyncMultiOutputSink(sinks)


def bootstrap_async_ast_validators() -> None:
    """Bootstrap AST validators from both async and sync plugins."""
    load_all_async_plugins()
    validators = _flatten(plugin_manager.hook.register_ast_validators())

    # Also load sync validators
    from osprey.worker.adaptor.plugin_manager import load_all_osprey_plugins, plugin_manager as sync_plugin_manager

    load_all_osprey_plugins()
    sync_validators = _flatten(sync_plugin_manager.hook.register_ast_validators())

    registry = ValidatorRegistry.get_instance()
    seen = set()
    for validator in validators + sync_validators:
        if validator not in seen:
            seen.add(validator)
            registry.register_to_instance(validator)
