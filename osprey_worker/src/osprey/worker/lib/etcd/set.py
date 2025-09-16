from __future__ import absolute_import

import json
from typing import AbstractSet, Any, MutableSet

import gevent
from gevent.lock import RLock
from osprey.worker.lib import etcd


class ReadOnlyEtcdSet(AbstractSet[Any]):
    def __init__(
        self, etcd_key, etcd_client=None, lazy=True, item_encode_hook=None, item_decode_hook=None, serializer=json
    ):
        self._client = etcd_client or etcd.EtcdClient()
        self._etcd_key = etcd_key
        self._internal_set = set()
        self._set_lock = RLock()
        self._lazy = lazy
        self._loaded = False
        self._watcher_greenlet = None
        self._watcher = None
        self._item_encode_hook = item_encode_hook
        self._item_decode_hook = item_decode_hook
        self._serializer = serializer

        if not lazy:
            self._load()

    def watch(self):
        """
        Starts the watcher for the ETCd key.
        """
        if not self._loaded:
            self._load()

        if self._watcher_greenlet is None:
            self._watcher_greenlet = gevent.spawn(self._loop)
            return True

        return False

    def stop_watching(self):
        """
        Kills the watcher that's watching the key. The set may then be stale.
        """
        if self._watcher_greenlet:
            self._watcher_greenlet.kill()
            self._watcher_greenlet = None
            return True

        return False

    @property
    def _set(self):
        if not self._loaded:
            self._load()

        return self._internal_set

    @_set.setter
    def _set(self, new_set):
        self._loaded = True
        self._internal_set = new_set

    def _loop(self):
        try:
            if not self._watcher:
                self._watcher = self._client.get_watcher(self._etcd_key, recursive=False)

            for event in self._watcher.continue_watching():
                self._handle_event(event)

        finally:
            self._watcher = None
            self._watcher_greenlet = None

    # noinspection PyMethodMayBeStatic
    def _serialize_item(self, item):
        if self._item_encode_hook:
            return self._item_encode_hook(item)

        return item

    # noinspection PyMethodMayBeStatic
    def _deserialize_item(self, item):
        if self._item_decode_hook:
            return self._item_decode_hook(item)

        return item

    def _load(self, force=False):
        with self._set_lock:
            if self._loaded and not force:
                return

            watcher = self._client.get_watcher(self._etcd_key, recursive=False)
            self._handle_event(watcher.begin_watching())
            self._watcher = watcher

    def _handle_event(self, event):
        if isinstance(event, etcd.FullSyncOne):
            self._set = self._deserialize(event.value)

        elif isinstance(event, etcd.FullSyncOneNoKey):
            self._set = set()

    def _deserialize(self, value):
        new_set = set()
        for item in self._serializer.loads(str(value)):
            new_set.add(self._deserialize_item(item))

        return new_set

    def __contains__(self, item):
        return item in self._set

    def __iter__(self):
        return iter(self._set)

    def __eq__(self, other):
        if isinstance(other, EtcdSet):
            other = other._set

        return isinstance(other, set) and self._set == other

    def __len__(self):
        return len(self._set)


class EtcdSet(ReadOnlyEtcdSet, MutableSet[Any]):
    def add(self, item):
        """
        Atomically adds an item to a set and returns True/False whether or not the item was added.
        """
        return self._cas_atomic_update(lambda s: item not in s, lambda s: s.add(item))

    def discard(self, item):
        """
        Atomically removes an item from the set and returns True/False whether or not the item was discarded.
        """
        return self._cas_atomic_update(lambda s: item in s, lambda s: s.discard(item))

    def clear(self):
        """
        Atomically clears the set, returning True if the set had items prior to being cleared.
        """
        return self._cas_atomic_update(bool, lambda s: s.clear())

    def update(self, updater_fn):
        """
        Atomically updates the set by using an updater_fn, with the old set, that is expected to return a new set.
        The `updater_fn` may be called multiple times if there was a change to the underlying set while the updater
        function was running.

        :rtype None
        """

        def do_update(old_set):
            new_set = updater_fn(old_set)
            if not isinstance(new_set, set):
                raise TypeError('updater_fn must return a new set, and not %r' % new_set)
            return new_set

        return self._cas_atomic_update(lambda _: None, do_update)

    def _serialize(self, items=None):
        if items is None:
            items = self._set

        return self._serializer.dumps([self._serialize_item(item) for item in items])

    def _cas_atomic_update(self, before_state, updater):
        while True:
            kwargs = {}
            try:
                event = self._client.get(self._etcd_key)
                kwargs['prev_index'] = event.node.modified_index
                new_set = self._deserialize(event.node.value)
            except etcd.KeyNotFoundError:
                new_set = set()
                kwargs['prev_exist'] = False

            retval = before_state(new_set)
            updater_return_value = updater(new_set)
            if updater_return_value is not None:
                new_set = updater_return_value

            try:
                self._client.set(self._etcd_key, self._serialize(new_set), **kwargs)
                self._set = new_set
                return retval

            except etcd.CompareFailedError:
                gevent.sleep(0.05)
