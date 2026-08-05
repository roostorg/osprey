"""Regression tests for the continuous-consumption etcd watcher.

These tests cover the production incident where a worker's smite_shortlist instance
set got wiped to empty and never repopulated (select() raised "No service" forever
until the worker was restarted). The root cause was that the watch loop consumed
``continue_watching()`` one event at a time, recreating the generator per event,
which reset its resume index / dedup mux and defeated its built-in recovery.

The fix drives a single persistent ``continue_watching()`` generator in
``_watch_loop`` (one asyncio task per watcher, pumping ``next(it)`` through the
executor), applying each event on the loop thread.

Covered here:
  * recovery-after-empty-full-sync — an empty FullSyncRecursive wipes the set (no
    guard), and the SAME continuous generator repopulates it via incremental upserts;
  * stop() teardown — stop() cancels the watch task (and the ring's);
  * the same fix applied to _AsyncHashRing (SCALAR routing).
"""

import asyncio
import json
import queue
from types import SimpleNamespace

import pytest
from osprey.async_worker.lib.discovery.async_directory import AsyncServiceWatcher, _AsyncHashRing
from osprey.worker.lib.discovery.exceptions import ServiceUnavailable
from osprey.worker.lib.discovery.service import Service
from osprey.worker.lib.etcd import (
    FullSyncOne,
    FullSyncOneNoKey,
    FullSyncRecursive,
    IncrementalSyncDelete,
    IncrementalSyncUpsert,
)

# Sentinel pushed into a fake watcher's inbox to make continue_watching() return.
# After the watch task is cancelled, the executor thread is still parked in
# next() on inbox.get(); pushing this unblocks it so the pool thread is freed.
_UNBLOCK = object()


class FakeWatcher:
    """A controllable stand-in for an etcd watcher.

    ``begin_watching()`` returns a preset initial sync event. ``continue_watching()``
    is an infinite generator that yields whatever the test pushes into its inbox and
    only returns when ``_UNBLOCK`` is pushed (blocking on the inbox in between, like
    the real watcher blocking on etcd).
    """

    def __init__(self, initial_event):
        self.initial_event = initial_event
        self.inbox: 'queue.Queue' = queue.Queue()
        self.begin_count = 0
        self.continue_count = 0

    def begin_watching(self):
        self.begin_count += 1
        return self.initial_event

    def push(self, event):
        self.inbox.put(event)

    def unblock(self):
        """Release a blocked continue_watching() so the parked executor thread frees."""
        self.inbox.put(_UNBLOCK)

    def continue_watching(self):
        self.continue_count += 1
        while True:
            item = self.inbox.get()
            if item is _UNBLOCK:
                return
            yield item


class FakeEtcdClient:
    """Serves preconfigured FakeWatchers, routed by the ``recursive`` flag.

    AsyncServiceWatcher requests a recursive watcher (instances); _AsyncHashRing
    requests a non-recursive (scalar) watcher (ring). Extra requests beyond the
    provided pools get a benign idle watcher.
    """

    def __init__(self, recursive_watchers=None, scalar_watchers=None):
        self._recursive = list(recursive_watchers or [])
        self._scalar = list(scalar_watchers or [])
        self.calls = []  # list of (key, recursive)
        self.all_watchers = []

    def get_watcher(self, key, recursive=False, _use_mux=True):
        self.calls.append((key, recursive))
        pool = self._recursive if recursive else self._scalar
        if pool:
            watcher = pool.pop(0)
        else:
            default = FullSyncRecursive(items=[]) if recursive else FullSyncOneNoKey(key=key)
            watcher = FakeWatcher(initial_event=default)
        self.all_watchers.append(watcher)
        return watcher


# --- helpers -----------------------------------------------------------------


def _svc(host: str, port: int) -> Service:
    return Service(name='smite_shortlist', address=host, port=port)


def _node(value: str) -> SimpleNamespace:
    return SimpleNamespace(key=f'/k/{value}', value=value)


def _full_sync(services) -> FullSyncRecursive:
    return FullSyncRecursive(items=[_node(s.serialize()) for s in services])


def _upsert(service: Service) -> IncrementalSyncUpsert:
    return IncrementalSyncUpsert(key=f'/k/{service.id}', value=service.serialize())


def _delete(service: Service) -> IncrementalSyncDelete:
    return IncrementalSyncDelete(key=f'/k/{service.id}', prev_value=service.serialize())


def _ring_full_sync(members) -> FullSyncOne:
    return FullSyncOne(key='/discovery/svc/ring', value=json.dumps(list(members)))


async def _wait_until(predicate, timeout: float = 2.0, interval: float = 0.01) -> bool:
    """Poll ``predicate`` (yielding to the loop so the watch task can run)."""
    elapsed = 0.0
    while elapsed < timeout:
        if predicate():
            return True
        await asyncio.sleep(interval)
        elapsed += interval
    return predicate()


async def _teardown_service(watcher: AsyncServiceWatcher, client: FakeEtcdClient) -> None:
    await watcher.stop()
    # stop() cancels the watch tasks, but the executor threads driving next() are
    # still parked on the fake inboxes — release them so the pool threads are freed.
    for fw in client.all_watchers:
        fw.unblock()


# --- AsyncServiceWatcher -----------------------------------------------------


@pytest.mark.asyncio
async def test_recovers_after_empty_full_sync():
    """An empty FullSyncRecursive wipes the set; the same generator repopulates it.

    This is the incident: a key-deleted / index_cleared full sync emptied the instance
    set. With continuous consumption the subsequent incremental upsert (yielded by the
    SAME generator) restores the instance, so select() recovers without a restart.
    """
    svc = _svc('shortlist-a', 1001)
    watcher = FakeWatcher(initial_event=_full_sync([svc]))
    client = FakeEtcdClient(recursive_watchers=[watcher])

    asw = AsyncServiceWatcher(client, '/discovery', 'smite_shortlist')
    await asw.ensure_initialized()
    assert set(asw._instances) == {svc.id}

    # Empty full sync wipes the set (no guard — the set self-repopulates).
    watcher.push(_full_sync([]))
    assert await _wait_until(lambda: len(asw._instances) == 0)
    with pytest.raises(ServiceUnavailable):
        asw.select()

    # Incremental upsert on the same continuous generator repopulates the set.
    watcher.push(_upsert(svc))
    assert await _wait_until(lambda: svc.id in asw._instances)
    assert asw.select() is not None  # no longer raises "No service"

    await _teardown_service(asw, client)


@pytest.mark.asyncio
async def test_incremental_upsert_and_delete_applied():
    """Plain upsert/delete events flow through the watch loop and mutate the set."""
    a, b = _svc('a', 1), _svc('b', 2)
    watcher = FakeWatcher(initial_event=_full_sync([a]))
    client = FakeEtcdClient(recursive_watchers=[watcher])

    asw = AsyncServiceWatcher(client, '/discovery', 'smite_shortlist')
    await asw.ensure_initialized()
    assert set(asw._instances) == {a.id}

    watcher.push(_upsert(b))
    assert await _wait_until(lambda: set(asw._instances) == {a.id, b.id})

    watcher.push(_delete(a))
    assert await _wait_until(lambda: set(asw._instances) == {b.id})

    await _teardown_service(asw, client)


@pytest.mark.asyncio
async def test_stop_teardown():
    """stop() cancels the watch task for both the service watcher and its ring."""
    svc = _svc('a', 1)
    watcher = FakeWatcher(initial_event=_full_sync([svc]))
    client = FakeEtcdClient(recursive_watchers=[watcher])

    asw = AsyncServiceWatcher(client, '/discovery', 'smite_shortlist')
    await asw.ensure_initialized()

    watch_task = asw._watch_task
    ring_task = asw._ring._watch_task
    assert watch_task is not None and not watch_task.done()
    assert ring_task is not None and not ring_task.done()

    await asw.stop()

    assert watch_task.done()
    assert ring_task.done()

    # Free the executor threads still parked in next() on the fake inboxes.
    for fw in client.all_watchers:
        fw.unblock()


# --- _AsyncHashRing ----------------------------------------------------------


@pytest.mark.asyncio
async def test_ring_recovers_after_key_deleted():
    """Same latent bug on the SCALAR ring: a key-deleted full sync wipes members,
    and continuous consumption repopulates them from a later FullSyncOne."""
    key = '/discovery/svc/ring'
    ring_watcher = FakeWatcher(initial_event=_ring_full_sync(['a:1', 'b:2']))
    client = FakeEtcdClient(scalar_watchers=[ring_watcher])

    ring = _AsyncHashRing(client, key)
    await ring.ensure_initialized()
    assert len(ring._members) == 2

    # Key deleted -> ring wiped.
    ring_watcher.push(FullSyncOneNoKey(key=key))
    assert await _wait_until(lambda: len(ring._members) == 0)

    # Later value repopulates the ring on the same continuous generator.
    ring_watcher.push(FullSyncOne(key=key, value=json.dumps(['c:3'])))
    assert await _wait_until(lambda: len(ring._members) == 1)

    await ring.stop()
    ring_watcher.unblock()
