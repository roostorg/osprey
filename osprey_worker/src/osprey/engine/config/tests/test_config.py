import json
import textwrap
from typing import Dict
from unittest.mock import MagicMock

import pytest
from osprey.engine.ast.grammar import Source
from osprey.engine.ast.sources import CONFIG_PATH, SOURCE_ENTRY_POINT_PATH, Sources, SourcesConfig
from osprey.engine.ast_validator.validation_context import ValidatedSources
from osprey.engine.ast_validator.validator_registry import ValidatorRegistry
from osprey.engine.config.config_registry import ConfigRegistry, ConfigSubkey
from osprey.engine.config.config_subkey_handler import ConfigSubkeyHandler
from pydantic.error_wrappers import ValidationError as PydanticValidationError
from pydantic.main import BaseModel

from ...conftest import CheckFailureFunction, RunValidationFunction


class TestModel1(BaseModel):
    __test__ = False  # Tell pytest not to try to collect tests from this
    str_field: str = 'unset'


class TestModel2(BaseModel):
    __test__ = False  # Tell pytest not to try to collect tests from this
    int_field: int = -1


class TestModelNotRegistered(BaseModel):
    __test__ = False  # Tell pytest not to try to collect tests from this


@pytest.fixture()
def registry() -> ConfigRegistry:
    registry = ConfigRegistry()
    registry.register('test1', TestModel1)
    registry.register('test2', TestModel2)

    return registry


@pytest.fixture()
def handler(registry: ConfigRegistry) -> ConfigSubkeyHandler:
    return ConfigSubkeyHandler(registry, make_validated_sources_with_config({}))


def make_validated_sources_with_config(raw_config: Dict[str, object]) -> ValidatedSources:
    main = Source(path=SOURCE_ENTRY_POINT_PATH, contents='', actual_path=None)
    sources = Sources(
        sources={SOURCE_ENTRY_POINT_PATH: main},
        config=SourcesConfig(Source(path=CONFIG_PATH, contents=json.dumps(raw_config))),
    )
    return ValidatedSources(sources=sources, validation_results={}, warnings=[])


def test_config_registry_enforces_one_to_one_model_subkey(registry: ConfigRegistry) -> None:
    # Re-registering exact same should be okay
    registry.register('test1', TestModel1)
    # Registering different model on the same key should fail
    with pytest.raises(AssertionError):
        registry.register('test1', TestModel2)
    # Registering same model on a different key should fail
    with pytest.raises(AssertionError):
        registry.register('unused', TestModel1)


def test_config_registry_returns_registered_models(registry: ConfigRegistry) -> None:
    registry.register('test1', TestModel1)  # Re-register to ensure we de-duplicate

    assert len(registry.get_registered_subkeys()) == 2, registry.get_registered_subkeys()
    assert set(registry.get_registered_subkeys()) == {
        ConfigSubkey('test1', TestModel1),
        ConfigSubkey('test2', TestModel2),
    }


def test_config_registry_returns_individual_subkey_models(registry: ConfigRegistry) -> None:
    assert registry.has_model(TestModel1)
    assert registry.has_model(TestModel2)
    assert not registry.has_model(TestModelNotRegistered)


def test_config_registry_validator_has_full_path_in_separate_messages(
    registry: ConfigRegistry, run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    config_contents = textwrap.dedent(
        """
        test1:
            str_field: []
        test2:
            int_field: hello
        """
    ).strip()
    with check_failure():
        run_validation(
            {'main.sml': '', 'config.yaml': config_contents},
            validator_registry=ValidatorRegistry.instance_with_additional_validators(registry.get_validator()),
        )


def test_config_handler_calls_watcher_upon_register(registry: ConfigRegistry) -> None:
    handler = ConfigSubkeyHandler(registry, make_validated_sources_with_config({'test1': {'str_field': 'value'}}))
    watcher = MagicMock()

    handler.watch_config_subkey(TestModel1, watcher)

    assert watcher.call_count == 1
    assert watcher.call_args[0][0] == TestModel1(str_field='value')


def test_config_handler_dispatch_calls_all_functions(handler: ConfigSubkeyHandler) -> None:
    watcher1_1 = MagicMock()
    watcher1_2 = MagicMock()
    watcher2 = MagicMock()
    handler.watch_config_subkey(TestModel1, watcher1_1)
    handler.watch_config_subkey(TestModel1, watcher1_2)
    handler.watch_config_subkey(TestModel2, watcher2)
    watcher1_1.reset_mock()
    watcher1_2.reset_mock()
    watcher2.reset_mock()

    handler.dispatch_config(
        make_validated_sources_with_config({'test1': {'str_field': 'value'}, 'test2': {'int_field': 42}})
    )

    # Each called during dispatch, each with the right type of model
    assert watcher1_1.call_count == 1
    assert watcher1_1.call_args[0][0] == TestModel1(str_field='value')
    assert watcher1_2.call_count == 1
    assert watcher1_2.call_args[0][0] == TestModel1(str_field='value')
    assert watcher2.call_count == 1
    assert watcher2.call_args[0][0] == TestModel2(int_field=42)


def test_config_handler_checks_subkey_validity(handler: ConfigSubkeyHandler) -> None:
    # Check for missing subkey model for both watch and get.
    with pytest.raises(AssertionError):
        handler.watch_config_subkey(TestModelNotRegistered, lambda _: None)
    with pytest.raises(AssertionError):
        handler.get_config_subkey(TestModelNotRegistered)


def test_config_handler_has_known_good_config_updated_by_dispatch(registry: ConfigRegistry) -> None:
    # Takes the initial value
    handler = ConfigSubkeyHandler(registry, make_validated_sources_with_config({'test1': {'str_field': 'value 1'}}))
    assert handler.get_config_subkey(TestModel1).str_field == 'value 1'

    # Can update its value
    handler.dispatch_config(make_validated_sources_with_config({'test1': {'str_field': 'value 2'}}))
    assert handler.get_config_subkey(TestModel1).str_field == 'value 2'


def test_config_handler_throws_bad_config(registry: ConfigRegistry) -> None:
    handler = ConfigSubkeyHandler(registry, make_validated_sources_with_config({'test1': {'str_field': 'value'}}))
    watcher = MagicMock()
    handler.watch_config_subkey(TestModel1, watcher)
    watcher.reset_mock()

    # Bad config
    with pytest.raises(PydanticValidationError):
        handler.dispatch_config(make_validated_sources_with_config({'test1': {'str_field': []}}))

    # Watcher shouldn't have been called during dispatch
    assert watcher.call_count == 0
    # We should still have our known good config
    assert handler.get_config_subkey(TestModel1).str_field == 'value'


def test_config_rejects_extra_keys(
    registry: ConfigRegistry, run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    config_contents = textwrap.dedent(
        """
        test1:
            unknown_inner_key: "hey"

        unknown_top_level_key: "hello"
        """
    ).strip()
    with check_failure():
        run_validation(
            {'main.sml': '', 'config.yaml': config_contents},
            validator_registry=ValidatorRegistry.instance_with_additional_validators(registry.get_validator()),
        )
