from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Type

from osprey.engine.utils.types import add_slots
from pydantic import ValidationError as PydanticValidationError
from pydantic.main import BaseConfig, BaseModel, Extra

from .utils import parse_config_with_auto_default

if TYPE_CHECKING:
    from osprey.engine.ast_validator.base_validator import ConfigValidatorBase


@add_slots
@dataclass(frozen=True, eq=True)
class ConfigSubkey:
    """A simple container for both the string key name and the model used to parse a top-level subkey of the sources
    configuration."""

    name: str
    model_class: Type[BaseModel]


class ConfigRegistry:
    """Holds all of the subkeys for the sources configuration that we know how to validate and parse."""

    def __init__(self) -> None:
        self._subkey_model_to_name: dict[Type[BaseModel], str] = {}
        self._taken_subkey_names: set[str] = set()

    def clone(self) -> 'ConfigRegistry':
        clone = ConfigRegistry()
        for subkey in self.get_registered_subkeys():
            clone.register(subkey.name, subkey.model_class)
        return clone

    def register(self, subkey_name: str, model_class: Type[BaseModel]) -> None:
        """Register to handle a new subkey.

        Subkey names and subkey model classes have a unique 1-to-1 relationship. Each model can be registered for
        exactly one subkey name, and each subkey name can have exactly one model registered for it. It is safe to
        re-register the exact same name and model pair (doing so will no-op). If you need to use a model for multiple
        subkey names, just make (trivial) subclasses of it for each name. If we come across a scenario where we need
        different models for the same subkey name we can extend this class's functionality.
        """
        existing_subkey_name = self._subkey_model_to_name.get(model_class)
        if existing_subkey_name:
            # NOTE: Could be extended to allow multiple different model classes per subkey if we wanted.
            assert existing_subkey_name == subkey_name, (
                f'Cannot register a model class {model_class} for two different config subkeys.'
                f' Already had registered for {existing_subkey_name}, trying to register for {subkey_name}.'
                f' If you need the same model for different subkeys make a (trivial) subclass for each subkey.'
            )
        else:
            self._subkey_model_to_name[model_class] = subkey_name

    def get_validator(self) -> 'Type[ConfigValidatorBase]':
        """Create a class that can be used to validate a Sources object while creating a ValidatedSources.

        The class will go through each registered subkey and validate that the structure for that subkey is correct.
        """
        from osprey.engine.ast_validator.base_validator import ConfigValidatorBase
        from osprey.engine.ast_validator.validation_context import ValidationContext

        registry = self

        class ConfigValidator(ConfigValidatorBase):
            """Validates that a subkey of the config is compatible with a given Pydantic model."""

            def __init__(self, context: ValidationContext):
                super().__init__(context)
                self._parsed_config: dict[Type[BaseModel], BaseModel] | None = None

            def get_result(self) -> dict[Type[BaseModel], BaseModel]:
                assert self._parsed_config is not None, 'Should not call get_result if we had a validation failure'
                return self._parsed_config

            def run(self) -> None:
                parsed_config: dict[Type[BaseModel], BaseModel] = {}
                registered_subkey_names = set()
                for model, subkey_name in registry._subkey_model_to_name.items():
                    registered_subkey_names.add(subkey_name)
                    orig_base_config_extra = BaseConfig.extra
                    try:
                        # Default to failing if we have unknown/extra keys.
                        BaseConfig.extra = Extra.forbid

                        parsed_config[model] = parse_config_with_auto_default(
                            self.context.sources.config, subkey_name, model
                        )
                    except PydanticValidationError as e:
                        for error in e.errors():
                            error_location = [subkey_name, *error['loc']]
                            location_str = '.'.join(str(loc) for loc in error_location)
                            self.context.add_error(
                                message=f'invalid config value at `{location_str}`',
                                span=self.context.sources.config.closest_span_for_location(
                                    error_location, key_only=error['type'] == 'value_error.extra'
                                ),
                                hint=f'{error["msg"]} (type={error["type"]})',
                            )
                    finally:
                        # Reset this global state back to what it was.
                        BaseConfig.extra = orig_base_config_extra

                # Detect unknown top-level keys
                for top_level_key in self.context.sources.config:
                    if top_level_key not in registered_subkey_names:
                        self.context.add_error(
                            message=f'invalid top-level config key `{top_level_key}`',
                            span=self.context.sources.config.closest_span_for_location([top_level_key], key_only=True),
                            hint=f'make sure there is a config model registered for the `{top_level_key}` key',
                        )

                self._parsed_config = parsed_config

        return ConfigValidator

    def has_model(self, model_class: Type[BaseModel]) -> bool:
        """Returns whether or not the given model class has been registered."""
        return model_class in self._subkey_model_to_name

    def get_registered_subkeys(self) -> Sequence[ConfigSubkey]:
        """Returns all registered subkeys."""
        return [ConfigSubkey(name, model) for model, name in self._subkey_model_to_name.items()]
