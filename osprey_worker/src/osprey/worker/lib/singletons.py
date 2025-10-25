from typing import TYPE_CHECKING

from osprey.engine.config.config_registry import ConfigRegistry
from osprey.engine.stdlib import get_config_registry
from osprey.worker.lib.config import Config
from osprey.worker.lib.singleton import Singleton
from osprey.worker.lib.storage.labels import LabelsProvider

if TYPE_CHECKING:
    from osprey.worker.lib.osprey_engine import OspreyEngine

CONFIG: Singleton[Config] = Singleton(Config)
# Clone this so we don't pollute the stdlib registry with other things.
CONFIG_REGISTRY: Singleton[ConfigRegistry] = Singleton(lambda: get_config_registry().clone())


def _init_engine():
    from osprey.worker.lib.osprey_engine import bootstrap_engine

    return bootstrap_engine()


ENGINE: Singleton['OspreyEngine'] = Singleton(_init_engine)


def _init_labels_provider() -> LabelsProvider | None:
    """
    a helper method to initialize the labels provider for the LABELS_PROVIDER singleton
    """
    # the plugin manager imports this file to reference the labels provider singleton;
    # therefore, we need these to not cause circular imports
    from osprey.worker.adaptor.plugin_manager import (
        _labels_service_or_provider_is_registered,
        bootstrap_labels_provider,
    )
    # from osprey.worker.lib.singletons import CONFIG

    if not _labels_service_or_provider_is_registered():
        return None
    config = CONFIG.instance()
    return bootstrap_labels_provider(config)


LABELS_PROVIDER: Singleton['LabelsProvider | None'] = Singleton(_init_labels_provider)
"""
A Singleton that holds a `LabelsProvider`, if one is registered by the plugin manager.

If not, this Singleton will hold `None`. This makes it always safe to call `LABELS_PROVIDER.instance()`,
and enforces type checking rules when dealing with labels provider code, which may or may not be
supplied by users of Osprey.

An example use pattern might be:
```py
labels_provider: LabelsProvider | None = LABELS_PROVIDER.instance()
if labels_provider:
    # do labels provider things
```

Because this is a Singleton, implementers of `LabelsServiceBase` / `LabelsProvider` can implement statefulness
and expect that the statefulness will be present across all references within a given Osprey worker.
"""
