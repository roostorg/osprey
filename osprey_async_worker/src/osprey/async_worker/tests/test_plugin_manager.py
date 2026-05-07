"""Tests for the async worker plugin manager.

Locks down the behavior that bootstrap_async_udfs:
1. Resolves MXLookup to the async-native class (not sync stdlib).
2. Goes through the same register_udfs hook that third-party plugins use.
3. Doesn't drop other stdlib UDFs in the process.
"""

from __future__ import annotations

import pytest
from osprey.engine.stdlib.udfs.json_data import JsonData
from osprey.engine.stdlib.udfs.mx_lookup import MXLookup as SyncMXLookup
from osprey.engine.stdlib.udfs.rules import Rule

from osprey.async_worker.adaptor import plugin_manager as pm
from osprey.async_worker.stdlib_udfs import _async_stdlib_plugin
from osprey.async_worker.stdlib_udfs.async_mx_lookup import MXLookup as AsyncMXLookup


@pytest.fixture(autouse=True)
def reset_plugin_manager():
    """Clear lru_cache and unregister any plugins between tests.

    plugin_manager is a module-level singleton. Without this, state from
    one test (e.g. a registered plugin) leaks into the next.
    """
    pm.load_all_async_plugins.cache_clear()
    yield
    pm.load_all_async_plugins.cache_clear()
    if pm.plugin_manager.is_registered(_async_stdlib_plugin):
        pm.plugin_manager.unregister(_async_stdlib_plugin)


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
    for async_udf in _async_stdlib_plugin.register_udfs():
        assert async_udf.__name__ == 'MXLookup'  # currently the only override


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
