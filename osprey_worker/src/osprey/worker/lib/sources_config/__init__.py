from collections.abc import Callable
from typing import Type

from osprey.engine.config.config_registry import ConfigRegistry
from osprey.engine.config.config_subkey_handler import ModelT
from osprey.engine.utils.imports import import_all_direct_children
from osprey.worker.lib.singletons import CONFIG_REGISTRY


def register_config_subkey(subkey: str) -> Callable[[Type[ModelT]], Type[ModelT]]:
    """A decorator to register a Pydantic class to validate and parse a subkey of the config."""

    def _do_register(model: Type[ModelT]) -> Type[ModelT]:
        CONFIG_REGISTRY.instance().register(subkey, model)
        return model

    return _do_register


_has_initialized_config_registry = False


def get_config_registry() -> ConfigRegistry:
    """Returns a config registry with all known subkeys registered on it."""
    global _has_initialized_config_registry

    if not _has_initialized_config_registry:
        from osprey.worker.lib.sources_config import subkeys

        import_all_direct_children(subkeys)

        _has_initialized_config_registry = True

    return CONFIG_REGISTRY.instance()
