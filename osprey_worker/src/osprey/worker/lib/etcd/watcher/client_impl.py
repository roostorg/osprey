import logging
import time

from osprey.worker.lib.backoff import Backoff

from ._base import BaseWatcher
from ._events import FullSyncOne, FullSyncOneNoKey, FullSyncRecursive, IncrementalSyncDelete, IncrementalSyncUpsert
from ._mux import PassthruMux, RecursiveWatchMux, WatchMux

log = logging.getLogger('etcd.watcher')


class EtcdWatcherState(object):
    """
    Represents the state of the etcd watcher.
    """

    # The watcher is stopped.
    STOPPED = 'stopped'

    # The watcher is calling wait on etcd, waiting for updates to the key it is watching.
    WAIT = 'wait'

    # The watcher is trying to full sync the key with etcd. This can happen if there was no
    # initial sync (begin_watching(), was not called), or etcd simply fell behind and got
    # an event_index_cleared event.
    SYNC = 'sync'


class EtcdWatcher(BaseWatcher):
    """
    Implementation of the base watcher interface that talks directly to etcd.
    """

    def __init__(self, key, index=None, recursive=False, etcd_client=None, _use_mux=True):
        from osprey.worker.lib import etcd

        self._key = key
        self._index = index
        self._recursive = recursive
        self._client = etcd_client or etcd.EtcdClient()
        self._state = EtcdWatcherState.STOPPED
        self._backoff = Backoff(min_delay=1, max_delay=5)
        if _use_mux:
            self._mux = RecursiveWatchMux() if recursive else WatchMux()
        else:
            self._mux = PassthruMux()

    def __repr__(self):
        return 'EtcdWatcher(key=%r, recursive=%r, state=%r, index=%r)' % (
            self._key,
            self._recursive,
            self._state,
            self._index,
        )

    def begin_watching(self):
        if self._state != EtcdWatcherState.STOPPED:
            raise Exception('Watcher is already running.')

        initial_event, self._index = self._get_initial_event()
        self._mux.reset_with_initial_event(initial_event)
        return initial_event

    def continue_watching(self):
        if self._state != EtcdWatcherState.STOPPED:
            raise Exception('Watcher is already running.')

        # If we have an index, assume we are continuing our previous watch that was
        # started with `begin_watching(...).
        if self._index is not None:
            self._set_state(EtcdWatcherState.WAIT)
        else:
            # Otherwise, we are starting fresh, and will emit a sync event as our
            # first event.
            self._set_state(EtcdWatcherState.SYNC)

        try:
            # We are now beginning the watch cycle - and will be in 1 of 3 possible states:
            # - STOPPED: Stop watching.
            # - WAIT: Wait for updates from etcd going past `self._index`.
            # - SYNC: We need to re-sync fully with etcd.
            while self._state != EtcdWatcherState.STOPPED:
                if self._state == EtcdWatcherState.WAIT:
                    for event in self._do_watch_key():
                        yield event

                elif self._state == EtcdWatcherState.SYNC:
                    for event in self._do_sync():
                        yield event
        finally:
            # Something has interrupted this iterator, we can move to the stopped state now.
            self._set_state(EtcdWatcherState.STOPPED)
            self._index = None
            if not isinstance(self._mux, PassthruMux):
                self._mux = RecursiveWatchMux() if self._recursive else WatchMux()

    def _set_state(self, state):
        log.debug('%r transitioning to state %r', self, state)
        self._state = state

    def _do_sync(self):
        # noinspection PyBroadException
        try:
            initial_event, index = self._get_initial_event()
            self._backoff.succeed()
            self._index = index
            self._set_state(EtcdWatcherState.WAIT)
            for event in self._mux.dedupe_event(initial_event):
                yield event

        except MemoryError as e:
            raise e

        except Exception:
            delay = self._backoff.fail()
            log.exception('%r: etcd raised an error. sleeping for %.2f sec before retrying', self, delay)
            time.sleep(delay)

    def _gather_nodes_in_tree(self, root):
        if not root.nodes:  # leaf
            return [FullSyncOne.from_node(root)] if root.value is not None else []

        ret = []
        for child_node in root.nodes:  # subtree
            ret.extend(self._gather_nodes_in_tree(child_node))

        return ret

    def _get_initial_event(self):
        from osprey.worker.lib import etcd

        try:
            event = self._client.send_request('GET', self._key, {'recursive': self._recursive}, try_all_peers=True)
            index = event.etcd_index
            if self._recursive:
                initial_event = FullSyncRecursive(items=self._gather_nodes_in_tree(event.node))
            else:
                initial_event = FullSyncOne.from_node(event.node)

        except etcd.KeyNotFoundError as e:
            if self._recursive:
                initial_event = FullSyncRecursive(items=[])
            else:
                initial_event = FullSyncOneNoKey(self._key)
            index = e.index

        return initial_event, index

    def _do_watch_key(self):
        from osprey.worker.lib import etcd

        params = {'wait': True, 'waitIndex': self._index + 1}
        if self._recursive:
            params['recursive'] = True

        # noinspection PyBroadException
        try:
            event = self._client.send_request('GET', self._key, params, jitter_timeout=True)
            self._index = event.node.modified_index
            self._backoff.succeed()

            # Translation of event can yield multiple events.
            for event in self._translate_event(event):
                for deduped_event in self._mux.dedupe_event(event):
                    yield deduped_event

        except etcd.Timeout:
            return

        except etcd.EtcdError as e:
            if e.code == 401:  # event_index_cleared
                # When this error code is emitted from etcd, it means that the wait index has fallen too far behind,
                # and etcd is unable to fulfill the wait request. When this happens, we need to read the full key
                # from etcd again, and use the etcd index as the new wait index, which should be more recent.
                # To do this, we transition to the SYNC state, which will do a read of the latest key from
                # etcd, and then update `self._index`, so we can wait again.
                # NOTE: This will emit a full sync event from the iterator, and in some (if not most) cases,
                # it may emit a full-sync that doesn't actually have any changed data. It is up to the mux,
                # to de-duplicate these events to the downstream consumer.
                self._set_state(EtcdWatcherState.SYNC)
            else:
                delay = self._backoff.fail()
                log.exception(
                    '%r: etcd returned error code %s (%r), sleeping for %.2f sec before retrying',
                    self,
                    e.code,
                    e,
                    delay,
                )
                time.sleep(delay)

        except MemoryError as e:
            raise e

        except Exception:
            delay = self._backoff.fail()
            log.exception('%r: raised an error. sleeping for %.2f sec before retrying', self, delay)
            time.sleep(delay)

    def _translate_event(self, event):
        from osprey.worker.lib import etcd

        if self._recursive:
            if event.action in etcd.SET_ACTIONS:
                # Incremental sync on one key in a recursive watch.
                yield IncrementalSyncUpsert.from_node(event.node)
            elif event.action in etcd.DELETE_ACTIONS:
                # This means that a single key was deleted.
                if len(event.prev_node.key) > len(self._key):
                    yield IncrementalSyncDelete.from_node(event.node, event.prev_node)
                else:
                    # This means the root key was deleted, and we are trigger a full-sync
                    # to an empty list of items.
                    yield FullSyncRecursive(items=[])
        else:
            # If the key updates in a non-recursive watch, consider it a full sync, either
            # updating the value `FullSyncOne`, or deleting the value entirely `FullSyncOneNoKey`.
            if event.action in etcd.SET_ACTIONS:
                yield FullSyncOne.from_node(event.node)
            elif event.action in etcd.DELETE_ACTIONS:
                yield FullSyncOneNoKey(key=event.node.key)
