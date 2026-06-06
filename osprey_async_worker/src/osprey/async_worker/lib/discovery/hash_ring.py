"""Self-contained consistent hash ring for async-worker service discovery.

A small, dependency-free (stdlib-only) consistent hashing implementation used by
the async worker's etcd-backed service discovery. The API mirrors the subset the
discovery layer relies on:

  - ``HashRingNode(name: bytes, num_replicas: int)`` with ``.name`` / ``.num_replicas``
  - ``HashRing(default_num_replicas).add_nodes([...])``
  - ``HashRing.find_node(key: bytes) -> Optional[bytes]``        (the node *name*)
  - ``HashRing.find_nodes(key: bytes, count: int) -> List[bytes]`` (distinct names)

Placement is deterministic (MD5 over ``<name>:<replica>`` virtual points) so every
worker computes the same ring for the same membership. Exact placement is an
internal detail — only self-consistency across a fleet matters.
"""
from __future__ import annotations

import bisect
import hashlib
from typing import Dict, List, Optional, Sequence, Set


class HashRingNode:
    __slots__ = ('name', 'num_replicas')

    def __init__(self, name: bytes, num_replicas: int) -> None:
        self.name = name
        self.num_replicas = num_replicas


def _hash(data: bytes) -> int:
    # 64 bits of MD5 is plenty of spread for ring placement.
    return int.from_bytes(hashlib.md5(data).digest()[:8], 'big')


class HashRing:
    def __init__(self, default_num_replicas: int = 512) -> None:
        self._default_num_replicas = default_num_replicas
        self._points: List[int] = []  # sorted virtual-point hashes
        self._owner: Dict[int, bytes] = {}  # point hash -> node name
        self._names: Set[bytes] = set()

    def add_node(self, node: HashRingNode) -> None:
        if node.name in self._names:
            return
        self._names.add(node.name)
        replicas = node.num_replicas or self._default_num_replicas
        for i in range(replicas):
            point = _hash(node.name + b':' + str(i).encode())
            # Skip the rare collision rather than silently reassign ownership.
            if point not in self._owner:
                self._owner[point] = node.name
                bisect.insort(self._points, point)

    def add_nodes(self, nodes: Sequence[HashRingNode]) -> None:
        for node in nodes:
            self.add_node(node)

    def find_node(self, key: bytes) -> Optional[bytes]:
        if not self._points:
            return None
        idx = bisect.bisect(self._points, _hash(key)) % len(self._points)
        return self._owner[self._points[idx]]

    def find_nodes(self, key: bytes, count: int) -> List[bytes]:
        if not self._points or count <= 0:
            return []
        start = bisect.bisect(self._points, _hash(key))
        total = len(self._points)
        result: List[bytes] = []
        seen: Set[bytes] = set()
        for offset in range(total):
            name = self._owner[self._points[(start + offset) % total]]
            if name not in seen:
                seen.add(name)
                result.append(name)
                if len(result) >= count:
                    break
        return result
