import abc
import logging
from typing import Callable, Dict, Optional

from osprey.engine.ast.sources import Sources
from osprey.worker.lib.etcd import EtcdClient
from osprey.worker.lib.etcd.dict import ReadOnlyEtcdDict
from osprey.worker.lib.utils.input_stream_ready_signaler import InputStreamReadySignaler

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


class EtcdSourcesProvider(BaseSourcesProvider):
    """Provides sources which are dynamically updated by etcd, using a ReadOnlyEtcdDict."""

    def __init__(
        self,
        etcd_key: str,
        etcd_client: Optional[EtcdClient] = None,
        input_stream_ready_signaler: Optional[InputStreamReadySignaler] = None,
    ):
        self._sources_dict: ReadOnlyEtcdDict[str, str] = ReadOnlyEtcdDict(etcd_key=etcd_key, etcd_client=etcd_client)
        self._current_sources = Sources.from_dict(self._sources_dict.copy())
        self._sources_watcher_callback: Optional[SourcesWatcherCallback] = None
        self._input_stream_ready_signaler = input_stream_ready_signaler

        self._sources_dict.add_watcher(self._notify_watcher)
        self._sources_dict.watch()

    def _notify_watcher(self, sources_dict: Dict[str, str]) -> None:
        if self._input_stream_ready_signaler is not None:
            logging.info('Pausing input streams')
            self._input_stream_ready_signaler.pause_input_stream()
            self._input_stream_ready_signaler.wait_for_input_stream_to_pause()

        self._current_sources = Sources.from_dict(sources_dict)
        if self._sources_watcher_callback:
            self._sources_watcher_callback()

        if self._input_stream_ready_signaler is not None:
            logging.info('Restarting input streams')
            self._input_stream_ready_signaler.resume_input_stream()

    def get_current_sources(self) -> Sources:
        return self._current_sources

    def set_sources_watcher(self, callback: SourcesWatcherCallback) -> None:
        self._sources_watcher_callback = callback
