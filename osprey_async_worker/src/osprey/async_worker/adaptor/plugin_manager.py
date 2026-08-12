"""Plugin manager for the async worker.

Discovers plugins via the 'osprey_async_plugin' setuptools entry_point group.
All UDFs with I/O must have native async implementations — no sync fallbacks.

pattern: Imperative Shell
"""

from __future__ import annotations

import logging
import math
from functools import lru_cache
from typing import TYPE_CHECKING, Any, List, Type, cast

import pluggy
from osprey.async_worker.adaptor import hookspecs as async_hookspecs
from osprey.async_worker.adaptor.constants import OSPREY_ASYNC_ADAPTOR
from osprey.async_worker.adaptor.interfaces import AsyncBaseOutputSink, AsyncBatchableUDFBase, AsyncUDFBase
from osprey.async_worker.sinks.sink.output_sink import AsyncMultiOutputSink
from osprey.engine.ast_validator import ValidatorRegistry
from osprey.engine.executor.udf_execution_helpers import HasHelper, UDFHelpers
from osprey.engine.udf.base import UDFBase
from osprey.engine.udf.registry import UDFRegistry
from osprey.worker.lib.action_proto_deserializer import ActionProtoDeserializer

if TYPE_CHECKING:
    from osprey.worker.lib.config import Config

OSPREY_ASYNC_UDF_DEFAULT_TIMEOUT = 'OSPREY_ASYNC_UDF_DEFAULT_TIMEOUT'

hookimpl_osprey_async: pluggy.HookimplMarker = pluggy.HookimplMarker(OSPREY_ASYNC_ADAPTOR)

plugin_manager = pluggy.PluginManager(OSPREY_ASYNC_ADAPTOR)
plugin_manager.add_hookspecs(async_hookspecs)


def _flatten(seq: List[List[Any]]) -> List[Any]:
    return sum(seq, [])


@lru_cache(maxsize=1)
def load_all_async_plugins() -> None:
    """Load the first-party async-stdlib plugin and any third-party plugins.

    The first-party plugin (osprey.async_worker.stdlib_udfs._async_stdlib_plugin)
    contributes async-native replacements for sync stdlib UDFs (e.g. MXLookup).
    Third-party plugins are discovered via the 'osprey_async_plugin' setuptools
    entry_point group.
    """
    from osprey.async_worker.stdlib_udfs import _async_stdlib_plugin

    plugin_manager.register(_async_stdlib_plugin)
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


def _validate_udf_timeout(value: float) -> float:
    """Validate a resolved timeout value.

    Accepts valid positive finite values unchanged. Raises ValueError naming the
    config key and value when not finite or when non-positive.
    """
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f'config[{OSPREY_ASYNC_UDF_DEFAULT_TIMEOUT!r}] must be positive and finite, got {value}')
    return value


def bootstrap_async_udfs(config: 'Config | None' = None) -> tuple[UDFRegistry, UDFHelpers]:
    """Bootstrap UDFs from async plugins + stdlib.

    Loads stdlib UDFs (JsonData, StringLength, Rule, etc.) and async plugin UDFs.
    Plugin UDFs override stdlib UDFs with the same name — this is how async
    replacements (HasLabel, MXLookup, etc.) shadow their sync counterparts.
    Async-native replacements for sync stdlib UDFs come from the first-party
    `_async_stdlib_plugin`, which registers through the same `register_udfs`
    hook as third-party plugins. No sync fallbacks — all I/O UDFs must be
    native async.

    Resolves the default UDF timeout from OSPREY_ASYNC_UDF_DEFAULT_TIMEOUT config key,
    defaulting to 2.0. Validates before mutating process state.
    """
    from osprey.worker._stdlibplugin.udf_register import register_udfs as stdlib_register_udfs

    # Resolve and validate the configured timeout before any mutations
    resolved_timeout = 2.0 if config is None else config.get_float(OSPREY_ASYNC_UDF_DEFAULT_TIMEOUT, 2.0)
    _validate_udf_timeout(resolved_timeout)

    # Assign to both native base classes
    AsyncUDFBase.timeout = resolved_timeout
    AsyncBatchableUDFBase.timeout = resolved_timeout

    load_all_async_plugins()
    udf_helpers = UDFHelpers()

    stdlib_udfs = list(stdlib_register_udfs())
    plugin_udfs = _flatten(plugin_manager.hook.register_udfs())
    all_udfs = _deduplicate_udfs(stdlib_udfs, plugin_udfs)

    # Auto-register helpers for UDFs that extend HasHelper
    for udf in all_udfs:
        if issubclass(udf, HasHelper):
            udf_helpers.set_udf_helper(udf, udf.create_provider())

    # Plugin-provided helper bindings for UDFs whose helper depends on `config`
    # or wraps an external service. Each plugin returns `(udf_class, helper)`
    # pairs; the framework binds them without needing to import the UDF class.
    if config is not None:
        for udf_class, helper in _iter_plugin_udf_helpers(config):
            # Plugin-provided pairs are helper-bearing UDFs by contract, but the
            # collection is typed as the looser UDFBase; narrow for set_udf_helper.
            udf_helpers.set_udf_helper(cast('Type[HasHelper[Any]]', udf_class), helper)

    udf_registry = UDFRegistry.with_udfs(*all_udfs)
    return udf_registry, udf_helpers


def _iter_plugin_udf_helpers(config: Config) -> List[tuple[Type[UDFBase[Any, Any]], Any]]:
    """Collect `(udf_class, helper)` bindings from every plugin that implements
    the `register_udf_helpers` hook. Failures are logged and skipped so one
    broken plugin doesn't take down bootstrap.
    """
    pairs: List[tuple[Type[UDFBase[Any, Any]], Any]] = []
    if not hasattr(plugin_manager.hook, 'register_udf_helpers'):
        return pairs
    try:
        for plugin_result in plugin_manager.hook.register_udf_helpers(config=config):
            pairs.extend(plugin_result)
    except Exception:
        logging.exception('Failed to collect UDF helpers from plugins')
    return pairs


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
