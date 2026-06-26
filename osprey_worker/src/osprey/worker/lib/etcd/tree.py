from __future__ import print_function

import os
from collections import defaultdict
from collections.abc import Callable, Mapping
from multiprocessing import RLock

import gevent
from osprey.worker.lib import etcd

TWatcherCallback = Callable[[str, str | None], None]


class ReadOnlyEtcdTree(Mapping[str, str]):
    """
    This class provides tree semantics via recursively watching an etcd root key.
    Mainly there are a few differences compared to EtcdDict/EtcdSet:

    1. Clients of this class can register callbacks when a particular node path sees a CRUD event.
       These "node watchers" should be idempotent as we won't guarantee deduplication of redundant events.
    2. We don't try to deserialize the values, allowing clients to handle that.
    3. We override _handle_event to handle FullSyncRecursive, IncrementalSyncUpsert, and IncrementalSyncDelete
        events which are specific to recursive watches.
    """

    def __init__(self, root_path: str):
        assert root_path.startswith('/')

        self._root_path = root_path.rstrip('/')
        self._client = etcd.EtcdClient()
        self._loaded = False
        self._load_lock = RLock()
        self._watcher = None
        self._watcher_greenlet = None

        self._dict: dict[str, str] = {}
        """
        The dict holds the values, keys are the full absolute node path (i.e. starting with /example_path/...).

        Hey! Why isn't this a tree?

        Well, I did have a proper path prefix tree struture but later it turned out we didn't need full-fledged tree
        operations, just simple set/get/delete by path. So we're using a dict to simplify.
        """

        self._node_watchers: dict[str, list[TWatcherCallback]] = defaultdict(list)
        """
        Watchers for a specific node path e.g. /foo/bar/baz. The node path is the fully qualified path,
        i.e. self._root_path is the prefix.
        """

    def _load(self):
        if self._loaded:
            return

        with self._load_lock:
            watcher = self._client.get_watcher(self._root_path, recursive=True)
            self._handle_event(watcher.begin_watching())
            self._watcher = watcher
            self._loaded = True

    def _loop(self):
        try:
            if not self._watcher:
                self._watcher = self._client.get_watcher(self._root_path, recursive=True)

            for event in self._watcher.continue_watching():
                self._handle_event(event)
        finally:
            self._watcher = None
            self._watcher_greenlet = None

    def watch(self):
        if not self._loaded:
            self._load()

        if self._watcher_greenlet is None:
            self._watcher_greenlet = gevent.spawn(self._loop)

    def stop_watching(self):
        if self._watcher_greenlet:
            self._watcher_greenlet.kill()
            self._watcher_greenlet = None

    def _delete_subtree(self, path: str):
        """
        Delete all entries in self._dict whose key is prefixed by path.
        """

        keys_to_delete = [k for k in self._dict.keys() if k.startswith(path)]
        for key in keys_to_delete:
            del self._dict[key]

    def _handle_event(self, event: etcd.watcher._events.BaseEvent):
        if isinstance(event, etcd.FullSyncRecursive):
            if len(event.items) == 0:  # this means deleting the whole tree.
                self._dict = {}
                for node_path, watchers in self._node_watchers.items():
                    for watcher in watchers:
                        watcher(node_path, None)
            else:  # fully sync to the event, but inform watchers of deleted node paths
                new_dict = event.as_dict()

                for deleted_node_path in set(self._dict.keys()) - set(new_dict.keys()):
                    for watcher in self._node_watchers[deleted_node_path]:
                        watcher(deleted_node_path, None)

                self._dict = new_dict
                for node_path, value in self._dict.items():
                    for watcher in self._node_watchers[node_path]:
                        watcher(node_path, value)
        elif isinstance(event, etcd.IncrementalSyncUpsert):
            self._dict[event.key] = event.value
            for watcher in self._node_watchers[event.key]:
                watcher(event.key, event.value)
        elif isinstance(event, etcd.IncrementalSyncDelete):
            self._delete_subtree(event.key)
            for node_path, watchers in self._node_watchers.items():
                if node_path.startswith(event.key):
                    for watcher in watchers:
                        watcher(node_path, None)
        else:
            raise Exception('Unhandled event %s' % str(event))

    def watch_node(self, path_to_node: str, callback: TWatcherCallback):
        """Register a callback to be called when a node value at a given path is modified."""

        if not path_to_node.startswith(self._root_path):
            path_to_node = os.path.join(self._root_path, path_to_node.lstrip('/'))

        self._node_watchers[path_to_node].append(callback)

        self.watch()  # watch root if we're not already.

        # Inform callback of current value.
        callback(path_to_node, self.get(path_to_node))

    def __len__(self):
        return len(self._dict)

    def __iter__(self):
        return iter(self._dict)

    def __contains__(self, item: object):
        return item in self._dict

    def __getitem__(self, item: str):
        return self._dict[item]
