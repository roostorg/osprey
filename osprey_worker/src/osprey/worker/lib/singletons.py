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


def _init_engine() -> 'OspreyEngine':
    # Avoid circular imports
    from pathlib import Path

    from osprey.worker.adaptor.plugin_manager import bootstrap_ast_validators, bootstrap_udfs
    from osprey.worker.lib.data_exporters.validation_result_exporter import get_validation_result_exporter
    from osprey.worker.lib.osprey_engine import (
        OspreyEngine,
        get_sources_provider,
        should_yield_during_compilation,
    )

    udf_registry, _ = bootstrap_udfs()
    bootstrap_ast_validators()

    # Use static rules path if configured, otherwise use etcd
    config = CONFIG.instance()
    rules_path_str = config.get_str('OSPREY_RULES_PATH', '')
    rules_path = Path(rules_path_str) if rules_path_str else None

    return OspreyEngine(
        sources_provider=get_sources_provider(rules_path=rules_path),
        udf_registry=udf_registry,
        should_yield_during_compilation=should_yield_during_compilation(),
        validation_exporter=get_validation_result_exporter(),
    )


ENGINE: Singleton['OspreyEngine'] = Singleton(_init_engine)
