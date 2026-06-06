"""Async service discovery via etcd.

Pure asyncio replacement for osprey.worker.lib.discovery that avoids gevent.
Uses run_in_executor for the sync EtcdClient (etcd updates are infrequent).
The hash ring and service list are maintained in-memory with asyncio tasks
for background watching.
"""

import asyncio
import collections
import json
import logging
from random import randint, uniform
from time import time
from typing import Any, Callable, ClassVar, Deque, Dict, List, Optional, Tuple, Union

from hash_ring import HashRing, HashRingNode
from osprey.worker.lib import etcd
from osprey.worker.lib.discovery.exceptions import ServiceUnavailable
from osprey.worker.lib.discovery.service import Service
from osprey.worker.lib.etcd import EtcdClient

logger = logging.getLogger(__name__)

UP = 'up'
DOWN = 'down'

DEFAULT_SECONDARIES = 2
VISIBILITY_PERIOD_MAX_SEC = 15.0

ListenerFn = Callable[[str, Service], None]


class _AsyncHashRing:
    """Async-compatible hash ring backed by etcd.

    Reads the ring topology from etcd once, then watches for changes
    via a background asyncio task (sync etcd calls in run_in_executor).
    """

    def __init__(self, etcd_client: EtcdClient, key: str, default_num_replicas: int = 512) -> None:
        self._etcd_client = etcd_client
        self._key = key
        self._default_num_replicas = default_num_replicas
        self._ring: HashRing = HashRing(default_num_replicas)
        self._members: Dict[bytes, HashRingNode] = {}
        self._overrides: Dict[Any, Any] = {}
        self._initialized = False
        self._watch_task: Optional[asyncio.Task[None]] = None

    async def ensure_initialized(self) -> None:
        if self._initialized:
            return
        # Set early to prevent concurrent coroutines from double-initializing.
        self._initialized = True
        try:
            loop = asyncio.get_running_loop()
            watcher = await loop.run_in_executor(
                None, self._etcd_client.get_watcher, self._key, False
            )
            event = await loop.run_in_executor(None, watcher.begin_watching)
            self._handle_event(event)
            self._watch_task = asyncio.create_task(self._watch_loop(watcher))
        except Exception:
            self._initialized = False
            raise

    def select(self, key: Any, secondaries: int = 0) -> Any:
        if isinstance(key, str):
            key = key.encode()

        if secondaries == 0:
            override = self._overrides.get(key)
            if override and len(override) > 0:
                return override[0]
            return self._ring.find_node(key)

        nodes = self._ring.find_nodes(key, secondaries + 1)
        overrides = self._overrides.get(key)
        if overrides is not None:
            result = list(overrides)
            for node in nodes:
                if node not in result:
                    result.append(node)
            return result[:secondaries + 1]
        return nodes

    def _handle_event(self, event: Any) -> None:
        if isinstance(event, etcd.FullSyncOne):
            self._update_from_value(event.value)
        elif isinstance(event, etcd.FullSyncOneNoKey):
            self._ring = HashRing(self._default_num_replicas)
            self._members = {}
            self._overrides = {}

    def _update_from_value(self, value: str) -> None:
        data = json.loads(value)
        members: List[HashRingNode] = []
        overrides: Dict[Any, Any] = {}

        if isinstance(data, list):
            members = self._read_members(data)
        elif isinstance(data, dict) and data.get('schema') == 'v1':
            members = self._read_members(data['members'])
            overrides = self._read_overrides(data.get('overrides', []))

        ring = HashRing(self._default_num_replicas)
        ring.add_nodes(members)
        self._ring = ring
        self._members = {m.name: m for m in members}
        self._overrides = overrides

    def _read_members(self, raw: list) -> List[HashRingNode]:
        members = []
        for item in raw:
            if isinstance(item, str):
                members.append(HashRingNode(item.encode(), self._default_num_replicas))
            elif isinstance(item, dict):
                name = item['name']
                if isinstance(name, str):
                    name = name.encode()
                members.append(HashRingNode(name, int(item['num_replicas'])))
        return members

    @staticmethod
    def _read_overrides(raw: list) -> Dict[Any, Any]:
        result = {}
        for items in raw:
            if len(items) >= 2:
                key = items[0].encode() if isinstance(items[0], str) else items[0]
                values = [v.encode() if isinstance(v, str) else v for v in items[1:]]
                result[key] = values
        return result

    async def _watch_loop(self, watcher: Any) -> None:
        loop = asyncio.get_running_loop()
        try:
            while True:
                event = await loop.run_in_executor(None, self._blocking_next, watcher)
                if event is None:
                    break
                self._handle_event(event)
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception('hash ring watcher failed')

    @staticmethod
    def _blocking_next(watcher: Any) -> Any:
        """Blocking call — runs in thread pool."""
        try:
            for event in watcher.continue_watching():
                return event
        except Exception:
            return None
        return None

    async def stop(self) -> None:
        if self._watch_task:
            self._watch_task.cancel()
            try:
                await self._watch_task
            except asyncio.CancelledError:
                pass


class _ServiceWrapper:
    __slots__ = ('service', 'visible_at')

    def __init__(self, service: Service, visible_at: Optional[float]) -> None:
        self.service = service
        self.visible_at = visible_at

    def is_visible(self, tolerate_draining: bool = False) -> bool:
        if self.service.draining and not tolerate_draining:
            return False
        if self.visible_at is None:
            return True
        if self.visible_at < time():
            self.visible_at = None
            return True
        return False


class AsyncServiceWatcher:
    """Async replacement for ServiceWatcher.

    Watches etcd for service instance changes using asyncio tasks.
    Supports SCALAR routing (via hash ring) and ROUND_ROBIN (via rotation).
    """

    def __init__(self, etcd_client: EtcdClient, base_key: str, service_name: str) -> None:
        self._etcd_client = etcd_client
        self._key = f'{base_key}/{service_name}/instances'
        self._service_name = service_name
        self._instances: Dict[str, _ServiceWrapper] = {}
        self._rotation: Deque[str] = collections.deque()
        self._ring = _AsyncHashRing(etcd_client, f'{base_key}/{service_name}/ring')
        self._listeners: List[ListenerFn] = []
        self._initialized = False
        self._watch_task: Optional[asyncio.Task[None]] = None

    async def ensure_initialized(self) -> None:
        if self._initialized:
            return
        # Set early to prevent concurrent coroutines from double-initializing.
        self._initialized = True
        try:
            loop = asyncio.get_running_loop()
            watcher = await loop.run_in_executor(
                None, self._etcd_client.get_watcher, self._key, True
            )
            event = await loop.run_in_executor(None, watcher.begin_watching)
            self._handle_full_sync(event, delay_visibility=False)
            logger.info(
                'async watcher %s: %d instances loaded: %s',
                self._service_name, len(self._instances), list(self._instances.keys()),
            )
            await self._ring.ensure_initialized()
            self._watch_task = asyncio.create_task(self._watch_loop(watcher))
        except Exception:
            self._initialized = False
            raise

    def select(
        self,
        selector: Any = None,
        secondaries: int = DEFAULT_SECONDARIES,
        instances_to_skip: int = 0,
        tolerate_draining: bool = False,
    ) -> Service:
        if selector is None or callable(selector):
            self._rotation.rotate()
            not_yet_visible = []
            for service_id in self._rotation:
                wrapper = self._instances[service_id]
                if not selector or selector(wrapper.service):
                    if wrapper.is_visible(tolerate_draining):
                        return wrapper.service
                    elif not wrapper.service.draining:
                        not_yet_visible.append(wrapper.service)
            if not_yet_visible:
                from random import choice
                return choice(not_yet_visible)
            raise ServiceUnavailable(f'No service for {self._service_name}')
        else:
            members = self._ring.select(selector, secondaries)
            if not isinstance(members, list):
                members = [members]
            for member in members[instances_to_skip:]:
                member_str = member.decode() if isinstance(member, bytes) else str(member)
                wrapper = self._instances.get(member_str)
                if wrapper and (tolerate_draining or not wrapper.service.draining):
                    return wrapper.service
            raise ServiceUnavailable(
                f'No service for {self._service_name} key={selector} '
                f'ring_members={[m.decode() if isinstance(m, bytes) else m for m in members]} '
                f'instances={list(self._instances.keys())}'
            )

    def select_all(self, tolerate_draining: bool = False) -> List[Service]:
        services = []
        for service_id in self._rotation:
            wrapper = self._instances[service_id]
            if wrapper.is_visible(tolerate_draining):
                services.append(wrapper.service)
        return services if services else [w.service for w in self._instances.values() if not w.service.draining]

    def add_lazy_listener(self, listener: ListenerFn) -> None:
        self._listeners.append(listener)

    def _handle_full_sync(self, event: Any, delay_visibility: bool = True) -> None:
        if not isinstance(event, etcd.FullSyncRecursive):
            return
        latest = {}
        for value in event.values():
            instance = Service.deserialize(value)
            latest[instance.id] = instance
        for instance_id in list(self._instances.keys()):
            if instance_id not in latest:
                self._remove_instance(instance_id)
        for instance in latest.values():
            self._add_instance(instance, delay_visibility)

    def _add_instance(self, new_instance: Service, delay_visibility: bool = True) -> None:
        if new_instance.id not in self._instances:
            idx = randint(0, len(self._rotation))
            self._rotation.insert(idx, new_instance.id)
            self._instances[new_instance.id] = _ServiceWrapper(
                service=new_instance,
                visible_at=(time() + uniform(0, VISIBILITY_PERIOD_MAX_SEC)) if delay_visibility else None,
            )
            for listener in self._listeners:
                listener(UP, new_instance)
            logger.debug('async discovery: + %s@%s', new_instance.name, new_instance.id)
        else:
            existing = self._instances[new_instance.id]
            existing.service.merge(new_instance)
            if existing.service.id not in self._rotation:
                self._rotation.insert(randint(0, len(self._rotation)), new_instance.id)

    def _remove_instance(self, instance_id: str) -> None:
        wrapper = self._instances.pop(instance_id, None)
        if wrapper is None:
            return
        if instance_id in self._rotation:
            self._rotation.remove(instance_id)
        for listener in self._listeners:
            listener(DOWN, wrapper.service)
        logger.debug('async discovery: - %s@%s', wrapper.service.name, instance_id)

    async def _watch_loop(self, watcher: Any) -> None:
        loop = asyncio.get_running_loop()
        try:
            while True:
                event = await loop.run_in_executor(None, self._blocking_next, watcher)
                if event is None:
                    break
                if isinstance(event, etcd.IncrementalSyncUpsert):
                    self._add_instance(Service.deserialize(event.value))
                elif isinstance(event, etcd.IncrementalSyncDelete):
                    self._remove_instance(Service.deserialize(event.prev_value).id)
                elif isinstance(event, etcd.FullSyncRecursive):
                    self._handle_full_sync(event)
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception('async service watcher failed for %s', self._service_name)

    @staticmethod
    def _blocking_next(watcher: Any) -> Any:
        try:
            for event in watcher.continue_watching():
                return event
        except Exception:
            return None
        return None

    async def stop(self) -> None:
        if self._watch_task:
            self._watch_task.cancel()
            try:
                await self._watch_task
            except asyncio.CancelledError:
                pass
        await self._ring.stop()


class AsyncDirectory:
    """Async replacement for Directory.

    Singleton that creates AsyncServiceWatcher instances per service name.
    Uses the sync EtcdClient via run_in_executor for etcd reads.
    """

    _instances: ClassVar[Dict[Tuple[Any, ...], 'AsyncDirectory']] = {}

    @classmethod
    def instance(cls, secure: bool = False) -> 'AsyncDirectory':
        key = (secure,)
        if key not in cls._instances:
            cls._instances[key] = cls(secure=secure)
        return cls._instances[key]

    def __init__(self, base_key: str = '/discovery', secure: bool = False) -> None:
        self._base_key = base_key
        self._etcd_client = EtcdClient(secure=secure)
        self._watchers: Dict[str, AsyncServiceWatcher] = {}

    def get_watcher(self, service_name: str) -> AsyncServiceWatcher:
        if service_name not in self._watchers:
            self._watchers[service_name] = AsyncServiceWatcher(
                self._etcd_client, self._base_key, service_name
            )
        return self._watchers[service_name]

    def select_all(self, service_name: str) -> List[Service]:
        watcher = self.get_watcher(service_name)
        return watcher.select_all()

    async def stop(self) -> None:
        for watcher in self._watchers.values():
            await watcher.stop()
