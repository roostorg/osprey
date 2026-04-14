"""Async sources provider for the async worker.

Port of osprey.worker.lib.sources_provider with asyncio instead of gevent.
The etcd watcher runs in a thread pool (run_in_executor) since the underlying
etcd client is synchronous. This is acceptable because etcd updates are
infrequent (rule deployments, not per-request).
"""

import asyncio
import json
import logging
import random
from typing import Callable, Dict, Optional

from osprey.engine.ast.sources import Sources
from osprey.worker.lib.etcd import EtcdClient, FullSyncOne, FullSyncOneNoKey
from osprey.worker.lib.sources_provider_base import BaseSourcesProvider

SourcesWatcherCallback = Callable[[], None]


class AsyncInputStreamReadySignaler:
    """Async version of InputStreamReadySignaler.

    Uses asyncio.Event instead of gevent.event.Event for pause/resume signaling.
    """

    def __init__(self) -> None:
        self._event = asyncio.Event()
        self._event.set()  # Start in "ready" state

    def should_pause_input_stream(self) -> bool:
        return not self._event.is_set()

    async def pause_input_stream(self) -> None:
        await asyncio.sleep(random.uniform(0, 600))  # Same jitter as gevent version
        self._event.clear()

    def resume_input_stream(self) -> None:
        self._event.set()

    async def wait_until_resume(self) -> None:
        await self._event.wait()


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
        input_stream_ready_signaler: Optional[AsyncInputStreamReadySignaler] = None,
    ):
        self._etcd_key = etcd_key
        self._client = etcd_client or EtcdClient()
        self._current_sources: Optional[Sources] = None
        self._sources_watcher_callback: Optional[SourcesWatcherCallback] = None
        self._input_stream_ready_signaler = input_stream_ready_signaler
        self._watcher = None
        self._watcher_task: Optional[asyncio.Task] = None

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
                    self._watcher = await loop.run_in_executor(
                        None, self._client.get_watcher, self._etcd_key, False
                    )

                # Block in thread pool waiting for next etcd event
                try:
                    event = await loop.run_in_executor(None, self._get_next_event)
                    backoff = 1.0  # Reset on success
                except StopIteration:
                    # Watcher exhausted, restart
                    self._watcher = None
                    continue
                except Exception:
                    logging.exception('Error in etcd watcher loop, retrying in %.1fs', backoff)
                    self._watcher = None
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 30.0)
                    continue

                if event is not None:
                    await self._handle_event(event)
        except asyncio.CancelledError:
            return
        finally:
            self._watcher = None

    def _get_next_event(self):
        """Get the next event from the watcher iterator. Runs in thread pool."""
        return next(iter(self._watcher.continue_watching()))

    async def _handle_event(self, event) -> None:
        """Handle an etcd event by updating sources and notifying watchers."""
        sources_dict = self._parse_event(event)

        if self._input_stream_ready_signaler is not None:
            logging.info('Pausing input streams')
            await self._input_stream_ready_signaler.pause_input_stream()

        self._current_sources = Sources.from_dict(sources_dict)
        if self._sources_watcher_callback:
            self._sources_watcher_callback()

        if self._input_stream_ready_signaler is not None:
            logging.info('Restarting input streams')
            self._input_stream_ready_signaler.resume_input_stream()

    def get_current_sources(self) -> Optional[Sources]:
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
