from collections import defaultdict
from typing import TYPE_CHECKING, Any, Callable, Type, TypeVar

from pydantic.main import BaseModel

from .config_registry import ConfigRegistry
from .utils import parse_config_with_auto_default

if TYPE_CHECKING:
    from osprey.engine.ast_validator.validation_context import ValidatedSources

ModelT = TypeVar('ModelT', bound=BaseModel)


class ConfigSubkeyHandler:
    """Keeps track of latest parsed configs and dispatches config updates to watchers."""

    def __init__(self, config_registry: ConfigRegistry, initial_sources: 'ValidatedSources') -> None:
        self._config_registry = config_registry
        self._config_subkey_handlers: dict[Type[BaseModel], list[Callable[[Any], None]]] = defaultdict(list)
        # Holds the parsed configs
        self._known_good_parsed_config = self._parse_new_config(initial_sources)

    def _validate_subkey_registered(self, model_class: Type[BaseModel]) -> None:
        assert self._config_registry.has_model(model_class), 'Must register config subkey models before using them!'

    def _parse_new_config(self, validated_sources: 'ValidatedSources') -> dict[Type[BaseModel], BaseModel]:
        raw_config = validated_sources.sources.config

        # First parse and validate all config subkeys
        parsed_subkeys = {}
        for subkey in self._config_registry.get_registered_subkeys():
            parsed_subkeys[subkey.model_class] = parse_config_with_auto_default(
                raw_config, subkey.name, subkey.model_class
            )

        return parsed_subkeys

    def watch_config_subkey(self, model_class: Type[ModelT], update_callback: Callable[[ModelT], None]) -> None:
        """Register to watch for updates to the given subkey.

        Upon receiving a new valid configuration the watcher will be called with the parsed model object. Note that
        the configuration object a watcher receives may not actually be different from the one it got immediately
        previously. The given model class must already have been registered on the underlying ConfigRegister class.
        """
        self._validate_subkey_registered(model_class)

        self._config_subkey_handlers[model_class].append(update_callback)
        # Do an initial dispatch to the listener
        update_callback(self.get_config_subkey(model_class))

    def get_config_subkey(self, model_class: Type[ModelT]) -> ModelT:
        """Returns the parsed model object for the subkey.

        This method works from already parsed data and is therefore suitable to put in a hot path. The given model
        class must already have been registered on the underlying ConfigRegister class.
        """
        self._validate_subkey_registered(model_class)
        parsed = self._known_good_parsed_config[model_class]
        assert isinstance(parsed, model_class)
        return parsed

    def dispatch_config(self, validated_sources: 'ValidatedSources') -> None:
        """Update the internal known-good config and tell all watchers about (potentially) new configuration values."""
        self._known_good_parsed_config = self._parse_new_config(validated_sources)

        # Then dispatch the updated objects to the callbacks
        for model_class, subkey_handlers in self._config_subkey_handlers.items():
            for callback in subkey_handlers:
                callback(self._known_good_parsed_config[model_class])
