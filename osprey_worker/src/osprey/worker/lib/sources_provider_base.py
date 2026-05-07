"""Base sources provider classes — no gevent/etcd dependency.

These classes are used by both the sync (gevent) and async (asyncio) workers.
The gevent-dependent EtcdSourcesProvider remains in sources_provider.py.
"""

import abc
from pathlib import Path
from typing import Callable

from osprey.engine.ast.sources import Sources

SourcesWatcherCallback = Callable[[], None]


class BaseSourcesProvider(abc.ABC):
    """Provides an interface to get the and be informed of current sources of rules which the rules engine should
    evaluate"""

    @abc.abstractmethod
    def get_current_sources(self) -> Sources:
        raise NotImplementedError

    @abc.abstractmethod
    def set_sources_watcher(self, callback: SourcesWatcherCallback) -> None:
        raise NotImplementedError


class StaticSourcesProvider(BaseSourcesProvider):
    """Provides a static sources that won't change for the lifetime of the provider."""

    def __init__(self, sources: Sources):
        self._sources = sources

    def get_current_sources(self) -> Sources:
        return self._sources

    def set_sources_watcher(self, callback: SourcesWatcherCallback) -> None:
        return None
