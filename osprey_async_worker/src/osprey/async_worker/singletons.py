"""Singletons for the async worker.

Mirrors osprey.worker.lib.singletons but for the async engine. Services that
migrate from the gevent engine to the async engine often rely on a process-wide
ENGINE.instance() accessor; this module supplies the async equivalent.

The gevent ENGINE singleton lives at osprey.worker.lib.singletons.ENGINE and
holds an OspreyEngine. This module's ENGINE holds an AsyncOspreyEngine and is
distinct — they share the underlying execution graph machinery, but each owns
its own compile thread pool, sources provider, and config subkey state.
"""

from pathlib import Path

from osprey.async_worker.adaptor.plugin_manager import (
    bootstrap_async_ast_validators,
    bootstrap_async_udfs,
    bootstrap_validation_exporter,
)
from osprey.async_worker.engine import AsyncOspreyEngine
from osprey.engine.ast.sources import Sources
from osprey.worker.lib.singleton import Singleton
from osprey.worker.lib.singletons import CONFIG
from osprey.worker.lib.sources_provider_base import BaseSourcesProvider, StaticSourcesProvider


def _resolve_sources_provider() -> BaseSourcesProvider:
    """Build a sources provider from config.

    Mirrors osprey.worker.lib.osprey_engine.get_sources_provider. If
    OSPREY_RULES_PATH is set, returns a StaticSourcesProvider; otherwise
    returns a (sync) EtcdSourcesProvider keyed by OSPREY_ETCD_SOURCES_PROVIDER_KEY.

    Note: we reuse the *sync* EtcdSourcesProvider rather than the async one
    because the factory is called synchronously on first ENGINE.instance().
    The async etcd provider requires `await provider.start()`, which can't
    run from a sync singleton initializer. For services that need the async
    etcd provider with a live watch (e.g. the async worker itself), construct
    AsyncOspreyEngine directly and don't go through this singleton.
    """
    config = CONFIG.instance()
    rules_path_str = config.get_optional_str('OSPREY_RULES_PATH')
    if rules_path_str:
        return StaticSourcesProvider(sources=Sources.from_path(Path(rules_path_str)))

    # EtcdSourcesProvider is imported locally because its module chain
    # (osprey.worker.lib.etcd.*) does `import gevent`. Keeping it off the
    # module-level import set means consumers that only ever hit the
    # OSPREY_RULES_PATH branch never pull gevent into sys.modules.
    from osprey.worker.lib.sources_provider import EtcdSourcesProvider

    etcd_key = config.get_str('OSPREY_ETCD_SOURCES_PROVIDER_KEY', '/config/osprey/rules-sink-sources')
    return EtcdSourcesProvider(etcd_key=etcd_key)


def _init_engine() -> AsyncOspreyEngine:
    """Factory for the ENGINE singleton.

    Bootstraps async UDFs + AST validators, builds a sources provider from
    config, and constructs an AsyncOspreyEngine. Mirrors the gevent
    bootstrap_engine_with_helpers wiring.
    """
    config = CONFIG.instance()
    udf_registry, _udf_helpers = bootstrap_async_udfs(config=config)
    bootstrap_async_ast_validators()

    validation_exporter = bootstrap_validation_exporter(config)

    return AsyncOspreyEngine(
        sources_provider=_resolve_sources_provider(),
        udf_registry=udf_registry,
        should_yield_during_compilation=config.get_bool('OSPREY_PERIODIC_YIELD_DURING_COMPILATION', False),
        validation_exporter=validation_exporter,
    )


ENGINE: Singleton[AsyncOspreyEngine] = Singleton(_init_engine)
