from typing import TYPE_CHECKING

from osprey.engine.config.config_registry import ConfigRegistry
from osprey.engine.stdlib import get_config_registry
from osprey.worker.lib.config import Config
from osprey.worker.lib.singleton import Singleton

if TYPE_CHECKING:
    from osprey.worker.lib.osprey_engine import OspreyEngine

CONFIG: Singleton[Config] = Singleton(Config)
# Clone this so we don't pollute the stdlib registry with other things.
CONFIG_REGISTRY: Singleton[ConfigRegistry] = Singleton(lambda: get_config_registry().clone())


def _init_engine():
    from osprey.worker.lib.osprey_engine import bootstrap_engine

    return bootstrap_engine()


ENGINE: Singleton['OspreyEngine'] = Singleton(_init_engine)
