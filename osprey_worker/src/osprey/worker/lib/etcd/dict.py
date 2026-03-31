from __future__ import absolute_import

import json
from collections.abc import Callable, Mapping, MutableMapping
from multiprocessing import RLock
from typing import TypeVar

import gevent
from osprey.worker.lib import etcd

K = TypeVar('K')
V = TypeVar('V')


class ReadOnlyEtcdDict(Mapping[K, V]):
    def __init__(self, etcd_key: str, etcd_client: etcd.EtcdClient | None = None, lazy=True, serializer=json):
        self._client = etcd_client or etcd.EtcdClient()
        self._etcd_key = etcd_key
        self._lazy = lazy
        self._loaded = False
        self._watcher = None
        self._watcher_greenlet: gevent.Greenlet | None = None
        self._serializer = serializer
        self._internal_dict: dict[str, str] = {}
        self._load_lock = RLock()
        self._watchers: list[Callable[[dict[str, str]], None]] = []

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
    def _dict(self):
        if not self._loaded:
            self._load()

        return self._internal_dict

    @_dict.setter
    def _dict(self, new_dict):
        self._loaded = True
        self._internal_dict = new_dict

    def _loop(self):
        try:
            if self._watcher is None:
                self._watcher = self._client.get_watcher(self._etcd_key, recursive=False)

            for event in self._watcher.continue_watching():
                self._handle_event(event)

        finally:
            self._watcher = None
            self._watcher_greenlet = None

    def _handle_event(self, event):
        if isinstance(event, etcd.FullSyncOne):
            new_dict = self._serializer.loads(str(event.value))
            self._dict = new_dict
            self._notify_watchers()
        elif isinstance(event, etcd.FullSyncOneNoKey):
            self._dict = {}
            self._notify_watchers()

    def _load(self, force=False):
        with self._load_lock:
            if self._loaded and not force:
                return

            watcher = self._client.get_watcher(self._etcd_key, recursive=False)
            initial_event = watcher.begin_watching()
            self._handle_event(initial_event)
            self._watcher = watcher

    def _notify_watchers(self):
        for watcher in self._watchers:
            watcher(self._dict)

    def add_watcher(self, watcher: Callable[[dict[str, str]], None]):
        self._watchers.append(watcher)

    def __len__(self):
        return len(self._dict)

    def __contains__(self, item):
        return item in self._dict

    def __iter__(self):
        return iter(self._dict)

    def __getitem__(self, item):
        return self._dict[item]

    def copy(self):
        """Returns a shallow copy of the underlying dict."""
        return self._dict.copy()


class EtcdDict(ReadOnlyEtcdDict[K, V], MutableMapping[K, V]):
    def __delitem__(self, key):
        def updater(new_dict):
            del new_dict[key]
            return new_dict

        self._cas_atomic_update(updater)

    def __setitem__(self, key, value):
        def updater(new_dict):
            new_dict[key] = value
            return new_dict

        self._cas_atomic_update(updater)

    def clear(self):
        self._cas_atomic_update(lambda _: {})

    def replace_with(self, new_dict: dict[str, str]):
        assert isinstance(new_dict, dict)
        self._cas_atomic_update(lambda _: new_dict)

    def _cas_atomic_update(self, updater):
        while True:
            kwargs = {}
            try:
                event = self._client.get(self._etcd_key)
                kwargs['prev_index'] = event.node.modified_index
                new_dict = self._serializer.loads(str(event.node.value))
            except etcd.KeyNotFoundError:
                new_dict = {}
                kwargs['prev_exist'] = False

            updater_return_value = updater(new_dict)
            if updater_return_value is not None:
                new_dict = updater_return_value

            try:
                self._client.set(self._etcd_key, self._serializer.dumps(new_dict), **kwargs)
                self._dict = new_dict
                return

            except etcd.CompareFailedError:
                gevent.sleep(0.05)
