from typing import TYPE_CHECKING

from osprey.engine.utils.imports import import_all_direct_children

if TYPE_CHECKING:
    from osprey.engine.config.config_registry import ConfigRegistry


def get_config_registry() -> 'ConfigRegistry':
    from . import configs

    import_all_direct_children(configs)

    from ._registry import get_config_registry

    return get_config_registry()


__all__ = ['get_config_registry']
