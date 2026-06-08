"""Async sources provider for the async worker.

Port of osprey.worker.lib.sources_provider with asyncio instead of gevent.
The etcd watcher runs in a thread pool (run_in_executor) since the underlying
etcd client is synchronous. This is acceptable because etcd updates are
infrequent (rule deployments, not per-request).
"""

import asyncio
import inspect
import json
import logging
import random
from typing import Any, Awaitable, Callable, Dict, Iterator, Optional, Union

from osprey.engine.ast.sources import Sources
from osprey.worker.lib.etcd import BaseWatcher, EtcdClient, FullSyncOne, FullSyncOneNoKey
from osprey.worker.lib.sources_provider_base import BaseSourcesProvider

# The async engine's _handle_updated_sources is a coroutine function so the
# compile can run in a thread pool while the event loop continues servicing
# in-flight tasks. We accept either a sync or async callable for back-compat.
SourcesWatcherCallback = Callable[[], Union[None, Awaitable[None]]]


class AsyncEtcdSourcesProvider(BaseSourcesProvider):
    """Provides sources dynamically updated by etcd, using asyncio.

    The etcd client is synchronous, so watch operations are offloaded to
    a thread pool via run_in_executor. This is fine because etcd updates
    happen infrequently (rule deployments).
    """

    def __init__(
        self,
        etcd_key: str,
        etcd_client: Optional[EtcdClient] = None,
        reload_jitter_seconds: float = 0,
    ):
        self._etcd_key = etcd_key
        self._client = etcd_client or EtcdClient()
        self._current_sources: Optional[Sources] = None
        self._sources_watcher_callback: Optional[SourcesWatcherCallback] = None
        self._reload_jitter_seconds = reload_jitter_seconds
        self._watcher: Optional[BaseWatcher] = None
        # Long-lived iterator over the watcher's event stream. continue_watching()
        # is a generator function — every call creates a new generator with a
        # fresh WatchMux and a reset _index, which defeats the watcher's built-in
        # dedup of redundant FullSyncOne events. Iterate one generator persistently
        # to match how ReadOnlyEtcdDict drives the gevent watcher.
        self._watcher_iter: Optional[Iterator[Any]] = None
        self._watcher_task: Optional[asyncio.Task[None]] = None

    async def start(self) -> None:
        """Initialize sources from etcd and start watching for changes."""
        loop = asyncio.get_running_loop()

        # Initial load in thread pool (sync etcd client)
        initial_dict = await loop.run_in_executor(None, self._load_initial)
        self._current_sources = Sources.from_dict(initial_dict)

        # Start watcher loop as an async task
        self._watcher_task = asyncio.create_task(self._watch_loop())

    def _load_initial(self) -> Dict[str, str]:
        """Load initial sources from etcd. Runs in thread pool."""
        watcher = self._client.get_watcher(self._etcd_key, recursive=False)
        initial_event = watcher.begin_watching()
        self._watcher = watcher
        return self._parse_event(initial_event)

    def _parse_event(self, event) -> Dict[str, str]:
        """Parse an etcd event into a sources dict."""
        if isinstance(event, FullSyncOne):
            return json.loads(str(event.value))
        elif isinstance(event, FullSyncOneNoKey):
            return {}
        return {}

    async def _watch_loop(self) -> None:
        """Watch for etcd changes, running the sync watcher in a thread pool."""
        loop = asyncio.get_running_loop()
        backoff = 1.0
        try:
            while True:
                if self._watcher is None:
                    self._watcher = await loop.run_in_executor(None, self._client.get_watcher, self._etcd_key, False)
                    self._watcher_iter = None
                if self._watcher_iter is None:
                    assert self._watcher is not None
                    self._watcher_iter = self._watcher.continue_watching()

                # Block in thread pool waiting for next etcd event. Drive the
                # SAME generator each iteration — the watcher's WatchMux dedups
                # consecutive identical FullSyncOne events (which are common
                # post-rule-deploy as etcd's wait API re-syncs), but the dedup
                # state lives on the generator. Recreating the generator per
                # event would defeat the dedup and put the loop into a tight
                # SYNC re-fetch loop on the main asyncio thread.
                watcher_iter = self._watcher_iter
                try:
                    event = await loop.run_in_executor(None, lambda: next(watcher_iter))
                    backoff = 1.0  # Reset on success
                except StopIteration:
                    # Generator exhausted (e.g. transient etcd error inside
                    # continue_watching). Reopen the watcher fresh.
                    self._watcher = None
                    self._watcher_iter = None
                    continue
                except Exception:
                    logging.exception('Error in etcd watcher loop, retrying in %.1fs', backoff)
                    self._watcher = None
                    self._watcher_iter = None
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 30.0)
                    continue

                if event is not None:
                    await self._handle_event(event)
        except asyncio.CancelledError:
            return
        finally:
            self._watcher = None
            self._watcher_iter = None

    async def _handle_event(self, event) -> None:
        """Handle an etcd event by updating sources and notifying watchers."""
        sources_dict = self._parse_event(event)
        new_sources = Sources.from_dict(sources_dict)

        # Etcd watcher reconnects and session refreshes re-deliver the current
        # value as a FullSyncOne event, so we see many events where the
        # content is unchanged. Skip recompile when the sources hash matches
        # what we already have — recompile transiently doubles peak memory and
        # can push pods over the OOM line.
        if self._current_sources is not None and new_sources.hash() == self._current_sources.hash():
            return

        # Keep the stream connected while the engine serves its old graph; the engine
        # gates executions only for the final graph swap.
        if self._reload_jitter_seconds > 0:
            await asyncio.sleep(random.uniform(0, self._reload_jitter_seconds))

        self._current_sources = new_sources
        if self._sources_watcher_callback:
            result = self._sources_watcher_callback()
            if inspect.isawaitable(result):
                await result

    # NOTE: BaseSourcesProvider.get_current_sources is typed -> Sources, but this
    # provider legitimately returns None before start() loads from etcd (see
    # test_provider_get_current_sources_default_none). Widening the base return type
    # to Optional[Sources] is the correct fix but lives in sources_provider_base.py
    # (a shared file outside this change). Keep the precise return type here.
    def get_current_sources(self) -> Optional[Sources]:  # type: ignore[override]
        return self._current_sources

    def set_sources_watcher(self, callback: SourcesWatcherCallback) -> None:
        self._sources_watcher_callback = callback

    async def stop(self) -> None:
        """Stop watching for etcd changes."""
        if self._watcher_task is not None:
            self._watcher_task.cancel()
            try:
                await self._watcher_task
            except asyncio.CancelledError:
                pass
            self._watcher_task = None
