import logging
from typing import Dict, Optional

from osprey.engine.ast.sources import Sources
from osprey.worker.lib.etcd import EtcdClient
from osprey.worker.lib.etcd.dict import ReadOnlyEtcdDict
from osprey.worker.lib.sources_provider_base import (
    BaseSourcesProvider,
    SourcesWatcherCallback,
    StaticSourcesProvider,
)
from osprey.worker.lib.utils.input_stream_ready_signaler import InputStreamReadySignaler

# Re-export base classes for backward compatibility
__all__ = [
    'BaseSourcesProvider',
    'StaticSourcesProvider',
    'SourcesWatcherCallback',
    'EtcdSourcesProvider',
]


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
        # The consumer (engine) compiles this initial snapshot at construction, so
        # seed the applied-hash to it — a later re-delivery of the same value is a
        # genuine no-op and should be skipped. See _notify_watcher for why dedup
        # keys off the *applied* hash rather than the last *received* one.
        self._applied_sources_hash: Optional[str] = self._current_sources.hash()

        self._sources_dict.add_watcher(self._notify_watcher)
        self._sources_dict.watch()

    def _notify_watcher(self, sources_dict: Dict[str, str]) -> None:
        new_sources = Sources.from_dict(sources_dict)

        # Etcd watcher reconnects and session refreshes re-deliver the current
        # value as a full snapshot, so we see many notifications where the content
        # is unchanged. Skip the (peak-memory-doubling) recompile only when the
        # consumer has ALREADY applied this exact hash. Comparing against the last
        # *applied* hash — not merely the last *received* one — keeps this
        # self-healing: if a recompile fails (engine keeps its old graph) or is
        # dropped (the async fire-and-forget bridge), _applied_sources_hash stays
        # behind, so the next re-delivery re-fires the recompile instead of being
        # suppressed and leaving the engine wedged on stale rules until restart.
        if self._applied_sources_hash is not None and new_sources.hash() == self._applied_sources_hash:
            return

        if self._input_stream_ready_signaler is not None:
            logging.info('Pausing input streams')
            self._input_stream_ready_signaler.pause_input_stream()
            self._input_stream_ready_signaler.wait_for_input_stream_to_pause()

        self._current_sources = new_sources
        if self._sources_watcher_callback:
            self._sources_watcher_callback()

        if self._input_stream_ready_signaler is not None:
            logging.info('Restarting input streams')
            self._input_stream_ready_signaler.resume_input_stream()

    def get_current_sources(self) -> Sources:
        return self._current_sources

    def set_sources_watcher(self, callback: SourcesWatcherCallback) -> None:
        self._sources_watcher_callback = callback

    def mark_sources_applied(self, sources_hash: str) -> None:
        # Advances the dedup baseline only once the consumer confirms it applied
        # these sources, so failed/dropped applies retry on the next re-delivery.
        self._applied_sources_hash = sources_hash
