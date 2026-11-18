"""Tests for choosing the execution result store when OSPREY_EXECUTION_RESULT_STORAGE_BACKEND=plugin."""

from collections.abc import Generator
from datetime import datetime
from typing import Any

import pytest
from osprey.engine.executor.execution_context import Action, ExecutionResult
from osprey.worker._stdlibplugin.execution_result_store_chooser import get_rules_execution_result_storage_backend
from osprey.worker._stdlibplugin.sink_register import register_output_sinks
from osprey.worker._stdlibplugin.storage_register import (
    register_execution_result_store as stdlib_register_execution_result_store,
)
from osprey.worker.adaptor.plugin_manager import hookimpl_osprey, plugin_manager
from osprey.worker.lib.config import Config
from osprey.worker.lib.singletons import CONFIG
from osprey.worker.lib.storage import ExecutionResultStorageBackendType, ExecutionResultStoreNotRegisteredError
from osprey.worker.lib.storage.stored_execution_result import ExecutionResultStore
from osprey.worker.sinks.sink.stored_execution_result_output_sink import StoredExecutionResultOutputSink


class InMemoryExecutionResultStore(ExecutionResultStore):
    def __init__(self) -> None:
        self.rows: dict[int, dict[str, Any]] = {}

    def select_one(self, action_id: int) -> dict[str, Any] | None:
        return self.rows.get(action_id)

    def select_many(self, action_ids: list[int]) -> list[dict[str, Any]]:
        return [self.rows[action_id] for action_id in action_ids if action_id in self.rows]

    def insert(
        self,
        action_id: int,
        extracted_features_json: str,
        error_traces_json: str,
        timestamp: datetime,
        action_data_json: str,
    ) -> None:
        self.rows[action_id] = {
            'id': action_id,
            'extracted_features': extracted_features_json,
            'error_traces': error_traces_json,
            'timestamp': timestamp,
            'action_data': action_data_json,
        }

    def delete(self, action_id: int) -> None:
        self.rows.pop(action_id, None)

    def delete_many(self, action_ids: list[int]) -> None:
        for action_id in action_ids:
            self.delete(action_id)


class StorePlugin:
    def __init__(self) -> None:
        self.store = InMemoryExecutionResultStore()

    @hookimpl_osprey
    def register_execution_result_store(self, config: Config) -> ExecutionResultStore:
        return self.store


class BrokenStorePlugin:
    @hookimpl_osprey
    def register_execution_result_store(self, config: Config) -> ExecutionResultStore:
        raise ConnectionError('store unreachable')


@pytest.fixture
def plugin_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('OSPREY_EXECUTION_RESULT_STORAGE_BACKEND', 'plugin')
    CONFIG.instance().unconfigure_for_tests()
    CONFIG.instance().configure_from_env()


@pytest.fixture
def store_plugin(plugin_backend: None) -> Generator[StorePlugin, None, None]:
    plugin = StorePlugin()
    plugin_manager.register(plugin)
    yield plugin
    plugin_manager.unregister(plugin)


def test_plugin_backend_returns_registered_store(store_plugin: StorePlugin) -> None:
    store = get_rules_execution_result_storage_backend(ExecutionResultStorageBackendType.PLUGIN)

    assert store is store_plugin.store


def test_plugin_backend_stores_execution_results(store_plugin: StorePlugin) -> None:
    sinks = register_output_sinks(CONFIG.instance())
    [sink] = [s for s in sinks if isinstance(s, StoredExecutionResultOutputSink)]

    action = Action(action_id=42, action_name='test_action', data={'k': 'v'}, timestamp=datetime(2026, 1, 1))
    sink.push(ExecutionResult(extracted_features={'Feature': 1}, action=action, effects={}, error_infos=[]))

    assert set(store_plugin.store.rows) == {42}
    assert store_plugin.store.rows[42]['action_data'] == '{"k": "v"}'


def test_plugin_backend_without_registered_store_raises(plugin_backend: None) -> None:
    with pytest.raises(ExecutionResultStoreNotRegisteredError):
        get_rules_execution_result_storage_backend(ExecutionResultStorageBackendType.PLUGIN)


def test_plugin_backend_surfaces_store_plugin_errors(plugin_backend: None) -> None:
    plugin = BrokenStorePlugin()
    plugin_manager.register(plugin)
    try:
        with pytest.raises(ConnectionError, match='store unreachable'):
            get_rules_execution_result_storage_backend(ExecutionResultStorageBackendType.PLUGIN)
    finally:
        plugin_manager.unregister(plugin)


def test_stdlib_hook_defers_to_other_plugins_for_plugin_backend(plugin_backend: None) -> None:
    assert stdlib_register_execution_result_store(config=CONFIG.instance()) is None
