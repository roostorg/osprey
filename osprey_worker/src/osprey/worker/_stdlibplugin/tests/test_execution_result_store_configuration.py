from unittest import mock

from osprey.worker._stdlibplugin import execution_result_store_chooser, sink_register, storage_register
from osprey.worker.lib.config import Config
from osprey.worker.lib.singletons import CONFIG
from osprey.worker.lib.storage import ExecutionResultStorageBackendType
from osprey.worker.lib.storage.stored_execution_result import bootstrap_execution_result_storage_service


def test_plugin_backend_returns_registered_store(monkeypatch) -> None:
    plugin_store = mock.MagicMock()
    monkeypatch.setattr(
        execution_result_store_chooser,
        'bootstrap_execution_result_store',
        mock.MagicMock(return_value=plugin_store),
    )

    store = execution_result_store_chooser.get_rules_execution_result_storage_backend(
        ExecutionResultStorageBackendType.PLUGIN
    )

    assert store is plugin_store


def test_storage_register_returns_none_when_storage_backend_is_unset(monkeypatch) -> None:
    fake_config = mock.MagicMock()
    fake_config.get_optional_str.return_value = None
    chooser = mock.MagicMock()
    monkeypatch.setattr(storage_register, 'get_rules_execution_result_storage_backend', chooser)

    store = storage_register.register_execution_result_store(config=fake_config)

    assert store is None
    chooser.assert_not_called()


def test_storage_register_defers_plugin_backend_to_external_plugins(monkeypatch) -> None:
    fake_config = mock.MagicMock()
    fake_config.get_optional_str.return_value = 'plugin'
    chooser = mock.MagicMock()
    monkeypatch.setattr(storage_register, 'get_rules_execution_result_storage_backend', chooser)

    store = storage_register.register_execution_result_store(config=fake_config)

    assert store is None
    chooser.assert_not_called()


def test_output_sinks_do_not_add_store_when_storage_backend_is_unset(monkeypatch) -> None:
    fake_config = mock.MagicMock()
    fake_config.get_bool.return_value = False
    fake_config.get_optional_str.return_value = None

    chooser = mock.MagicMock()
    monkeypatch.setattr(sink_register, 'get_rules_execution_result_storage_backend', chooser)

    sinks = sink_register.register_output_sinks(config=fake_config)

    chooser.assert_not_called()
    assert sinks == []


def test_output_sinks_add_store_when_storage_backend_is_explicit_minio(monkeypatch) -> None:
    fake_config = mock.MagicMock()
    fake_config.get_bool.return_value = False
    fake_config.get_optional_str.return_value = 'minio'

    chooser = mock.MagicMock(return_value=object())
    stored_sink = mock.MagicMock()
    monkeypatch.setattr(sink_register, 'get_rules_execution_result_storage_backend', chooser)
    monkeypatch.setattr(sink_register, 'StoredExecutionResultOutputSink', mock.MagicMock(return_value=stored_sink))

    sinks = sink_register.register_output_sinks(config=fake_config)

    chooser.assert_called_once_with(backend_type=ExecutionResultStorageBackendType.MINIO)
    assert sinks == [stored_sink]


def test_bootstrap_storage_service_requires_explicit_backend() -> None:
    with CONFIG.override_instance_for_test(Config({})):
        try:
            bootstrap_execution_result_storage_service()
        except AssertionError as exc:
            assert str(exc) == 'OSPREY_EXECUTION_RESULT_STORAGE_BACKEND is not configured'
        else:
            raise AssertionError('Expected bootstrap_execution_result_storage_service to fail when unset')


def test_bootstrap_storage_service_fails_when_storage_is_disabled() -> None:
    with CONFIG.override_instance_for_test(Config({'OSPREY_EXECUTION_RESULT_STORAGE_BACKEND': 'none'})):
        try:
            bootstrap_execution_result_storage_service()
        except AssertionError as exc:
            assert str(exc) == 'Execution result storage backend is disabled'
        else:
            raise AssertionError('Expected bootstrap_execution_result_storage_service to fail when disabled')
