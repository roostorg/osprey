from __future__ import annotations

import os
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, ClassVar

from osprey.worker.lib.discovery.service import Service
from osprey.worker.lib.discovery.service_watcher import ServiceWatcher
from osprey.worker.lib.etcd import EtcdClient
from osprey.worker.lib.etcd.ring import EtcdHashRing

if TYPE_CHECKING:
    from .service_watcher import SelectorFunctionType

    _InstancesKeyType = tuple[tuple[Any, ...], tuple[tuple[str, Any], ...]]


class Directory:
    _instances: ClassVar[dict[_InstancesKeyType, 'Directory']] = {}

    @classmethod
    def instance(cls, *args, **kwargs) -> Directory:
        """Discovery singleton."""
        key = (tuple(args), tuple(sorted(kwargs.items())))
        if key not in cls._instances:
            cls._instances[key] = cls(*args, **kwargs)
        return cls._instances[key]

    def __init__(self, base_key: str = '/discovery', etcd_client: EtcdClient | None = None, *args, **kwargs):
        # (...) -> None
        self.base_key = base_key
        self.etcd_client = etcd_client or EtcdClient(*args, **kwargs)

        self._watchers: dict[str, ServiceWatcher] = {}

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__}: {self.base_key}>'

    def register(self, service: Service, ttl: int) -> None:
        """Registers a service class."""
        self.etcd_client.set(self.key_for(service), value=service.serialize(), ttl=ttl)

    def unregister(self, service: Service) -> None:
        """Unregisters a service."""
        self.etcd_client.delete(self.key_for(service))

    @property
    def names(self) -> Iterable[str]:
        """Returns the names of all registered services."""
        event = self.etcd_client.get(self.base_key)
        rv = set()
        for node in event.node.nodes:
            rv.add(os.path.basename(node.key))
        return rv

    def select(self, name: str, selector: SelectorFunctionType | str | int | None = None) -> Service:
        """Selects an instance of a service based on a selector."""
        return self.get_watcher(name).select(selector)

    def select_all(self, name: str, selector: SelectorFunctionType | None = None) -> list[Service]:
        """Selects all instances of a service based on a selector."""
        return self.get_watcher(name).select_all(selector)

    def get_watcher(self, service_name: str, with_ring: bool = True) -> ServiceWatcher:
        """Creates a provider for a specific service."""
        # this code is intentionally optimized for cache hit speed
        # do not refactor to `if service_name in self._watchers`
        try:
            return self._watchers[service_name]
        except KeyError:
            ring = EtcdHashRing(self.etcd_client, f'{self.base_key}/{service_name}/ring') if with_ring else None
            watcher = self._watchers[service_name] = ServiceWatcher(self, self._key_for(service_name), ring)
            return watcher

    def key_for(self, service: Service) -> str:
        """Returns a path for a given service."""
        return self._key_for(service.name, service.id)

    def _key_for(self, service_name: str, service_id: str | None = None) -> str:
        """Returns a path for a service."""
        key = f'{self.base_key}/{service_name}/instances'
        if service_id is not None:
            key = f'{key}/{service_id}'
        return key
