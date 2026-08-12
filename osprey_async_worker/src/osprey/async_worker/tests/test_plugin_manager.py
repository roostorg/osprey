"""Tests for the async worker plugin manager.

Locks down the behavior that bootstrap_async_udfs:
1. Resolves MXLookup to the async-native class (not sync stdlib).
2. Goes through the same register_udfs hook that third-party plugins use.
3. Doesn't drop other stdlib UDFs in the process.
"""

from __future__ import annotations

import pytest
from osprey.async_worker.adaptor import plugin_manager as pm
from osprey.async_worker.adaptor.interfaces import AsyncBatchableUDFBase, AsyncUDFBase
from osprey.async_worker.stdlib_udfs import _async_stdlib_plugin
from osprey.async_worker.stdlib_udfs.async_mx_lookup import MXLookup as AsyncMXLookup
from osprey.engine.stdlib.udfs.json_data import JsonData
from osprey.engine.stdlib.udfs.labels import HasLabel as SyncHasLabel
from osprey.engine.stdlib.udfs.mx_lookup import MXLookup as SyncMXLookup
from osprey.engine.stdlib.udfs.rules import Rule
from osprey.worker.lib.config import Config


@pytest.fixture(autouse=True)
def reset_plugin_manager():
    """Clear lru_cache and unregister any plugins between tests.

    plugin_manager is a module-level singleton. Without this, state from
    one test (e.g. a registered plugin) leaks into the next.
    Restore both native base timeouts to 2.0 before and after each test.
    """
    pm.load_all_async_plugins.cache_clear()
    AsyncUDFBase.timeout = 2.0
    AsyncBatchableUDFBase.timeout = 2.0
    yield
    pm.load_all_async_plugins.cache_clear()
    if pm.plugin_manager.is_registered(_async_stdlib_plugin):
        pm.plugin_manager.unregister(_async_stdlib_plugin)
    AsyncUDFBase.timeout = 2.0
    AsyncBatchableUDFBase.timeout = 2.0


def test_async_stdlib_plugin_returns_async_mx_lookup() -> None:
    """The first-party plugin's register_udfs returns the async MXLookup directly."""
    udfs = list(_async_stdlib_plugin.register_udfs())
    assert AsyncMXLookup in udfs
    assert SyncMXLookup not in udfs


def test_async_stdlib_plugin_overrides_share_class_name() -> None:
    """Overrides shadow stdlib by class name — verify the assumption holds.

    _deduplicate_udfs matches by __name__, so the async override class must
    have the same __name__ as the sync class it replaces.
    """
    override_names = {async_udf.__name__ for async_udf in _async_stdlib_plugin.register_udfs()}
    assert override_names == {'MXLookup', 'HasLabel'}


def test_bootstrap_resolves_mx_lookup_to_async_version() -> None:
    registry, _helpers = pm.bootstrap_async_udfs(config=None)
    resolved = registry.get('MXLookup')
    assert resolved is AsyncMXLookup, (
        f'Expected MXLookup to resolve to AsyncMXLookup, got {resolved!r} '
        f'from module {resolved.__module__ if resolved else None}'
    )


def test_bootstrap_does_not_register_sync_mx_lookup() -> None:
    """Sync MXLookup must not appear in the merged registry under any name."""
    registry, _helpers = pm.bootstrap_async_udfs(config=None)
    for udf in registry.iter_functions():
        assert udf is not SyncMXLookup, 'Sync MXLookup leaked into the async registry'


def test_bootstrap_preserves_non_overridden_stdlib_udfs() -> None:
    """Stdlib UDFs without an async override should still be registered as-is."""
    registry, _helpers = pm.bootstrap_async_udfs(config=None)
    assert registry.get('JsonData') is JsonData
    assert registry.get('Rule') is Rule


def test_bootstrap_resolves_in_tree_async_udfs_to_native_classes() -> None:
    """bootstrap resolves in-tree async udfs to native classes"""
    registry, _helpers = pm.bootstrap_async_udfs(config=None)
    mx_lookup = registry.get('MXLookup')
    assert mx_lookup is AsyncMXLookup, f'expected AsyncMXLookup, got {mx_lookup!r}'
    assert hasattr(mx_lookup, 'is_native_async') and mx_lookup.is_native_async
    has_label = registry.get('HasLabel')
    assert has_label is not None, 'HasLabel not found'
    assert hasattr(has_label, 'is_native_async') and has_label.is_native_async


def test_merged_registry_excludes_replaced_sync_udfs() -> None:
    """merged registry excludes replaced sync udfs"""
    registry, _helpers = pm.bootstrap_async_udfs(config=None)
    for udf in registry.iter_functions():
        assert udf is not SyncMXLookup, 'sync MXLookup leaked into async registry'
        assert udf is not SyncHasLabel, 'sync HasLabel leaked into async registry'
    override_names = {udf.__name__ for udf in _async_stdlib_plugin.register_udfs()}
    assert override_names == {'MXLookup', 'HasLabel'}


def test_sync_stdlib_registration_keeps_sync_has_label() -> None:
    """sync stdlib registration keeps sync HasLabel"""
    from osprey.engine.udf.registry import UDFRegistry
    from osprey.worker._stdlibplugin.udf_register import register_udfs

    registry = UDFRegistry.with_udfs(*register_udfs())
    has_label = registry.get('HasLabel')
    assert has_label is SyncHasLabel, f'expected sync HasLabel, got {has_label!r}'


def test_bootstrap_registers_internal_plugin() -> None:
    """The internal async-stdlib plugin must be registered after bootstrap.

    This confirms the override flows through the pluggy hook system rather
    than a hardcoded path inside bootstrap_async_udfs.
    """
    pm.bootstrap_async_udfs(config=None)
    assert pm.plugin_manager.is_registered(_async_stdlib_plugin)


def test_bootstrap_register_udfs_hook_emits_async_mx_lookup() -> None:
    """The register_udfs hook itself returns AsyncMXLookup via the internal plugin."""
    pm.load_all_async_plugins()
    flattened: list = []
    for udfs in pm.plugin_manager.hook.register_udfs():
        flattened.extend(udfs)
    assert AsyncMXLookup in flattened


class _StubUDF:
    """A stand-in UDF class used to verify helper binding without depending on
    any concrete UDFBase subclass. Helper binding only stores the class as a
    dict key, so any hashable type works here."""


class _UDFHelpersPlugin:
    """A pluggy plugin that returns one (udf_class, helper) pair when
    register_udf_helpers is called."""

    def __init__(self, udf_class, helper, capture):
        self._udf_class = udf_class
        self._helper = helper
        self._capture = capture

    @pm.hookimpl_osprey_async
    def register_udf_helpers(self, config):
        self._capture.append(config)
        return [(self._udf_class, self._helper)]


def test_bootstrap_applies_register_udf_helpers_bindings() -> None:
    """A plugin that implements register_udf_helpers should have its (udf, helper)
    pair set on UDFHelpers during bootstrap. The framework must not need to
    import the plugin's UDF class to bind the helper."""
    helper = object()
    captured: list = []
    plugin = _UDFHelpersPlugin(_StubUDF, helper, captured)
    pm.plugin_manager.register(plugin)
    try:
        bound_config = Config({})
        _registry, helpers = pm.bootstrap_async_udfs(config=bound_config)
        assert captured == [bound_config], 'register_udf_helpers must receive the config'
        # UDFHelpers.get_udf_helper expects an instance (it calls type()).
        # Inspect the underlying dict directly since _StubUDF is not instantiable.
        assert helpers._helpers[_StubUDF] is helper
    finally:
        pm.plugin_manager.unregister(plugin)


def test_bootstrap_skips_helper_wiring_when_config_is_none() -> None:
    """register_udf_helpers depends on `config`; if no config is supplied,
    bootstrap must still succeed without invoking the hook."""
    captured: list = []
    plugin = _UDFHelpersPlugin(_StubUDF, object(), captured)
    pm.plugin_manager.register(plugin)
    try:
        _registry, helpers = pm.bootstrap_async_udfs(config=None)
        assert captured == [], 'hook must not be called when config is None'
        assert _StubUDF not in helpers._helpers
    finally:
        pm.plugin_manager.unregister(plugin)


def test_bootstrap_swallows_exceptions_from_register_udf_helpers() -> None:
    """A misbehaving plugin must not take down bootstrap. The exception is
    logged and other UDFs/helpers still load."""

    class _BrokenPlugin:
        @pm.hookimpl_osprey_async
        def register_udf_helpers(self, config):
            raise RuntimeError('plugin boom')

    plugin = _BrokenPlugin()
    pm.plugin_manager.register(plugin)
    try:
        bound_config = Config({})
        registry, _helpers = pm.bootstrap_async_udfs(config=bound_config)
        # Standard UDFs still resolved despite the broken hook.
        assert registry.get('JsonData') is JsonData
    finally:
        pm.plugin_manager.unregister(plugin)


def test_no_residual_register_labels_service_or_provider_hookspec() -> None:
    """The legacy labels-specific hookspec has been removed in favor of the
    generic register_udf_helpers hook."""
    pm.load_all_async_plugins()
    assert not hasattr(pm.plugin_manager.hook, 'register_labels_service_or_provider'), (
        'register_labels_service_or_provider should be removed in favor of register_udf_helpers'
    )


# AC4.1 test: absent configuration keeps both inherited native udf defaults at 2.0
def test_ac41_absent_config_keeps_defaults_at_two() -> None:
    """bootstrap with config=None and Config({}) leaves both native base defaults at 2.0."""
    # config=None case
    pm.bootstrap_async_udfs(config=None)
    assert AsyncUDFBase.timeout == 2.0
    assert AsyncBatchableUDFBase.timeout == 2.0

    # Config({}) case (initialized config with no timeout key)
    pm.bootstrap_async_udfs(config=Config({}))
    assert AsyncUDFBase.timeout == 2.0
    assert AsyncBatchableUDFBase.timeout == 2.0


# AC4.2 test: positive finite OSPREY_ASYNC_UDF_DEFAULT_TIMEOUT changes both defaults
def test_ac42_positive_finite_config_changes_defaults() -> None:
    """a numeric string under OSPREY_ASYNC_UDF_DEFAULT_TIMEOUT becomes the timeout on both native bases."""
    config = Config({'OSPREY_ASYNC_UDF_DEFAULT_TIMEOUT': '3.5'})
    pm.bootstrap_async_udfs(config=config)
    assert AsyncUDFBase.timeout == 3.5
    assert AsyncBatchableUDFBase.timeout == 3.5


# AC4.3 test: direct and inherited udf class timeout overrides continue to win over the configured default
def test_ac43_direct_and_inherited_overrides_win() -> None:
    """define one direct override and one concrete udf inheriting an override from an
    intermediate plugin base; bootstrap with a different configured default and assert
    both overrides remain unchanged."""

    class DirectOverrideUDF(AsyncUDFBase):
        timeout = 1.5

    class PluginBaseUDF(AsyncUDFBase):
        timeout = 2.5

    class InheritedOverrideUDF(PluginBaseUDF):
        pass  # Inherits timeout = 2.5

    config = Config({'OSPREY_ASYNC_UDF_DEFAULT_TIMEOUT': '5.0'})
    pm.bootstrap_async_udfs(config=config)

    # Native base defaults changed to 5.0
    assert AsyncUDFBase.timeout == 5.0
    assert AsyncBatchableUDFBase.timeout == 5.0

    # But direct and inherited overrides remain unchanged
    assert DirectOverrideUDF.timeout == 1.5
    assert InheritedOverrideUDF.timeout == 2.5


# AC4.4 test: malformed, non-positive, or non-finite configured values fail bootstrap
@pytest.mark.parametrize(
    'config_value,expected_exception',
    [
        ('not_a_number', TypeError),
        ('0', ValueError),
        ('-1.5', ValueError),
        ('nan', ValueError),
        ('inf', ValueError),
        ('-inf', ValueError),
    ],
)
def test_ac44_invalid_timeout_values_fail_bootstrap(config_value: str, expected_exception: type) -> None:
    """malformed, zero, negative, nan, inf, and -inf values fail bootstrap with appropriate errors."""
    config = Config({'OSPREY_ASYNC_UDF_DEFAULT_TIMEOUT': config_value})
    with pytest.raises(expected_exception, match='OSPREY_ASYNC_UDF_DEFAULT_TIMEOUT'):
        pm.bootstrap_async_udfs(config=config)
    # Verify state unchanged
    assert AsyncUDFBase.timeout == 2.0
    assert AsyncBatchableUDFBase.timeout == 2.0


# AC4.5 test: repeated bootstrap resets without leaking a prior value
def test_ac45_repeated_bootstrap_resets_without_leaking() -> None:
    """bootstrap with one configured value, then another, then config=None;
    assert each call replaces only the inherited base defaults solely through bootstrap behavior."""

    # First bootstrap with 3.0
    config1 = Config({'OSPREY_ASYNC_UDF_DEFAULT_TIMEOUT': '3.0'})
    pm.bootstrap_async_udfs(config=config1)
    assert AsyncUDFBase.timeout == 3.0
    assert AsyncBatchableUDFBase.timeout == 3.0

    # Second bootstrap with 4.5 (no manual reset; bootstrap must replace 3.0 with 4.5)
    config2 = Config({'OSPREY_ASYNC_UDF_DEFAULT_TIMEOUT': '4.5'})
    pm.bootstrap_async_udfs(config=config2)
    assert AsyncUDFBase.timeout == 4.5
    assert AsyncBatchableUDFBase.timeout == 4.5

    # Final bootstrap with config=None (no manual reset; bootstrap must restore 2.0)
    pm.bootstrap_async_udfs(config=None)
    assert AsyncUDFBase.timeout == 2.0
    assert AsyncBatchableUDFBase.timeout == 2.0
