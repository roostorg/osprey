from __future__ import absolute_import

import json
import logging
import random
from typing import Text

import gevent
import six
from gevent.lock import RLock
from gevent.threadpool import ThreadPool
from hash_ring import HashRing, HashRingNode
from osprey.worker.lib import etcd

log = logging.getLogger(__name__)

ring_builder_threadpool = ThreadPool(1)


class EtcdHashRingMember(HashRingNode):  # type: ignore
    def __init__(self, name: bytes, num_replicas: int):
        if num_replicas < 1:
            raise ValueError("can't create EtcdHashRingMember `%r` with num_replicas < 1 (%d)" % (name, num_replicas))
        super(EtcdHashRingMember, self).__init__(name, num_replicas)

    def __eq__(self, o):
        return isinstance(o, EtcdHashRingMember) and (self.name == o.name) and (self.num_replicas == o.num_replicas)

    def __repr__(self):
        return '<%s name:%s num_replicas:%s>' % (self.__class__.__name__, self.name, self.num_replicas)


class EtcdHashRing(object):
    def __init__(self, etcd_client, key, default_num_replicas=512):
        """
        :type etcd_client: etcd.EtcdClient
        :type key: str
        """
        self.etcd_client = etcd_client
        self.key = key

        self._lock = RLock()
        self._is_watching = False

        self._members = {}
        self._overrides = {}
        self._default_num_replicas = default_num_replicas
        self._ring = HashRing(default_num_replicas)

    def __repr__(self):
        return '<%s key:%s members:%s overrides:%s>' % (
            self.__class__.__name__,
            self.key,
            self._members.values(),
            self._overrides,
        )

    def __len__(self):
        self._ensure_watching()
        return len(self._members)

    def select(self, key, secondaries=0):
        """Selects a member in the ring based on key.

        :type key: str|int|unicode
        :type secondaries: int
        :rtype: str|[str]
        """
        self._ensure_watching()

        if isinstance(key, six.text_type):
            # Fix any unicode variants of key to be bytestrings
            key = six.ensure_binary(key)

        if secondaries == 0:
            return self._select_one_override(key) or self._ring.find_node(key)

        nodes = self._ring.find_nodes(key, secondaries + 1)
        overrides = self._select_overrides(key)

        if overrides is not None:
            for node in nodes:
                if node not in overrides:
                    overrides.append(node)

            return overrides[: secondaries + 1]
        else:
            return nodes

    def _select_one_override(self, key):
        if key in self._overrides:
            overrides = self._overrides[key]

            if len(overrides) > 0:
                return overrides[0]

        return None

    def _select_overrides(self, key):
        if key in self._overrides:
            return self._overrides[key]

        return None

    def add(self, *members):
        self._ensure_watching()

        dirty = False
        for member in members:
            member = EtcdHashRingTopology.read_member(member, self._default_num_replicas)
            # Only update members and mark as dirty if something actually changed
            if self._members.get(member.name) == member:
                continue
            self._members[member.name] = member
            dirty = True

        if dirty:
            self._persist_topology()

    def remove(self, *members):
        self._ensure_watching()

        dirty = False

        for member in members:
            member = EtcdHashRingTopology.read_member(member, self._default_num_replicas)
            if member.name not in self._members:
                log.warning("Couldn't remove unknown member: %r" % member)
                continue

            del self._members[member.name]
            dirty = True

        if dirty:
            self._persist_topology()

    def update(self, *members):
        self._ensure_watching()

        dirty = False

        for member in members:
            member = EtcdHashRingTopology.read_member(member, self._default_num_replicas)
            if member.name not in self._members:
                log.warning("Couldn't update unknown member: %r" % member)
                continue

            self._members[member.name] = member
            dirty = True

        if dirty:
            self._persist_topology()

    def _persist_topology(self):
        event = self.etcd_client.set(
            self.key, EtcdHashRingTopology.to_json(self.members, self.overrides)
        )  # , prev_index=self._etcd_index)
        topology = EtcdHashRingTopology.from_value(event.node.value, self._default_num_replicas)
        self._update(topology, immediate=True)

    @property
    def members(self):
        self._ensure_watching()
        return list(self._members.values())

    @property
    def overrides(self):
        self._ensure_watching()
        return self._overrides

    @overrides.setter
    def overrides(self, overrides):
        self._ensure_watching()
        if not isinstance(overrides, dict):
            raise ValueError('overrides must be a dict of {key => member-name}: %r.' % overrides)

        if self._overrides != overrides:
            self._overrides = overrides
            self._persist_topology()

    def _ensure_watching(self):
        if self._is_watching:
            return

        # Lock this to force anyone who needs a member to wait for initial list.
        with self._lock:
            # If this caller was just waiting for the lock, just return.
            if self._is_watching:
                return

            watcher = self.etcd_client.get_watcher(self.key, recursive=False)
            self._handle_event(watcher.begin_watching())
            self._is_watching = True
            gevent.spawn(self._watch, watcher)

    def _handle_event(self, event):
        if isinstance(event, etcd.FullSyncOne):
            self._update(EtcdHashRingTopology.from_value(event.value, self._default_num_replicas))
        elif isinstance(event, etcd.FullSyncOneNoKey):
            self._update(EtcdHashRingTopology())

    def _watch(self, watcher):
        try:
            for event in watcher.continue_watching():
                self._handle_event(event)
        finally:
            with self._lock:
                self._is_watching = False

    def _update(self, topology, immediate=False):
        def build_ring():
            ring = HashRing(self._default_num_replicas)
            ring.add_nodes(topology.members)
            return ring

        if not immediate and self._members and topology.members:
            gevent.sleep(random.randint(0, 500) / 1000)
        ring = ring_builder_threadpool.apply(build_ring)
        with self._lock:
            self._ring = ring
            self._members = {m.name: m for m in topology.members}
            self._overrides = topology.overrides


class EtcdHashRingTopology(object):
    def __init__(self, members=None, overrides=None):
        self.members = members or []
        self.overrides = overrides or {}

    def __repr__(self):
        return '<%s members:%s overrides:%s>' % (self.__class__.__name__, self.members, self.overrides)

    @staticmethod
    def from_value(value, default_num_replicas):
        """
        creates a ring topology from "value".

        this method accepts several inputs, and will transform them appropriately:

        1. a list of `EtcdHashRingMember` objects.
        2. a list of `{name, num_replicas}` dicts.
        3. a list of `str`, representing member names. `default_num_replicas` will be used.
        4. a "v1" ring topology definition, which takes the form of:

        {
            "schema": "v1",
            "members": [
                { "name": "guilds-1:3001", "num_replicas": 512 },
                { "name": "guilds-2:3001", "num_replicas": 512 },
                { "name": "sickbay0:3001", "num_replicas": 1 },
                { "name": "sickbay1:3001", "num_replicas": 1 }
            ],
            "overrides": [
                [100, "sickbay0:3001", "sickbay1:3001"]
            ]
        }
        """

        data = json.loads(value)

        if isinstance(data, list):
            members = EtcdHashRingTopology.read_members(data, default_num_replicas)
            overrides = {}

            return EtcdHashRingTopology(members, overrides)

        if isinstance(data, dict) and 'schema' in data:
            if data['schema'] == 'v1':
                members = EtcdHashRingTopology.read_members(data['members'], default_num_replicas)
                overrides = EtcdHashRingTopology.read_overrides(data['overrides'])

                return EtcdHashRingTopology(members, overrides)

        raise ValueError('%r cannot be transformed to a EtcdHashRingTopology.' % data)

    @staticmethod
    def from_members(value, default_num_replicas=None):
        members = EtcdHashRingTopology.read_members(value, default_num_replicas)

        return EtcdHashRingTopology(members)

    @staticmethod
    def read_members(members, default_num_replicas):
        hash_ring_members = []
        for member in members:
            try:
                hash_ring_members.append(EtcdHashRingTopology.read_member(member, default_num_replicas))
            except ValueError as e:
                log.error('Dropping invalid ring member: %s', e)
        return hash_ring_members

    @staticmethod
    def read_member(value, default_num_replicas):
        if isinstance(value, (str, six.text_type)):
            if default_num_replicas is None:
                raise Exception("couldn't construct string member: default_num_replicas was not provided.")

            return EtcdHashRingMember(name=six.text_type(value).encode(), num_replicas=default_num_replicas)

        if isinstance(value, dict):
            return EtcdHashRingMember(
                name=six.text_type(value['name']).encode(), num_replicas=int(value['num_replicas'])
            )

        if isinstance(value, EtcdHashRingMember):
            return value

        raise ValueError("can't create EtcdHashRingMember from value: %r." % value)

    @staticmethod
    def read_overrides(overrides):
        return dict([EtcdHashRingTopology.read_override(override) for override in overrides])

    @staticmethod
    def read_override(items):
        """
        :param items: a list of override items, prefixed with the key (`[key, values...]`). this exists because we
            cannot make overrides a map because json does not support non-string-keys.
        """

        assert len(items) >= 2

        items = [ensure_strings_are_bytes(x) for x in items]
        key = items[0]
        values = items[1:]

        return (key, values)

    @staticmethod
    def to_json(members, overrides=None):
        """
        writes `members` and `overrides` to the json schema that store into etcd.

        # remarks.

        for maximum backwards compatibility, we use the v0 "old-style" schema if there are no overrides, and the v1
        schema if there are overrides.

        the specific "v0", "v1" and legacy formats are described above in in `def from_value(..)`.
        """

        if not overrides:
            data = [EtcdHashRingTopology.write_member(member) for member in members]
        else:
            data = {
                'schema': 'v1',
                'members': [EtcdHashRingTopology.write_member(member) for member in members],
                'overrides': [EtcdHashRingTopology.write_override(override) for override in overrides.items()],
            }

        return json.dumps(data)

    @staticmethod
    def write_member(value):
        if isinstance(value, EtcdHashRingMember):
            return {
                'name': value.name if isinstance(value.name, str) else value.name.decode(),
                'num_replicas': value.num_replicas,
            }

        if isinstance(value, dict):
            return value

        raise ValueError("can't write ring member from value: %r." % value)

    @staticmethod
    def write_override(values):
        key = values[0]
        overrides = values[1]

        return [ensure_bytes_are_strings(x) for x in [key] + overrides]


def ensure_strings_are_bytes(value: int | str) -> bytes | int:
    if isinstance(value, str):
        return value.encode()

    return value


def ensure_bytes_are_strings(value: bytes | int) -> int | Text:
    if isinstance(value, bytes):
        return value.decode()

    return value
