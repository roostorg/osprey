from collections.abc import Callable
from typing import Type

from osprey.engine.config.config_registry import ConfigRegistry
from osprey.engine.config.config_subkey_handler import ModelT

_config_registry = ConfigRegistry()


def register_config_subkey(subkey: str) -> Callable[[Type[ModelT]], Type[ModelT]]:
    """A decorator to register a Pydantic class to validate and parse a subkey of the config."""

    def _do_register(model: Type[ModelT]) -> Type[ModelT]:
        _config_registry.register(subkey, model)
        return model

    return _do_register


def get_config_registry() -> ConfigRegistry:
    return _config_registry
