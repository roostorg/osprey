from __future__ import annotations

import logging
import weakref
from collections import deque
from collections.abc import Callable
from random import choice, randint, uniform
from time import time
from typing import TYPE_CHECKING

import gevent
import six
from gevent.lock import RLock
from osprey.worker.lib import etcd
from osprey.worker.lib.ddtrace_utils import trace
from osprey.worker.lib.discovery.exceptions import ServiceUnavailable
from osprey.worker.lib.discovery.service import Service
from typing_extensions import Literal

if TYPE_CHECKING:
    from osprey.worker.lib.discovery.directory import Directory
    from osprey.worker.lib.etcd.watcher import BaseWatcher

    SelectorFunctionType = Callable[[Service], bool]

log = logging.getLogger(__name__)

UP = 'up'
DOWN = 'down'

DEFAULT_SECONDARIES = 2
VISIBLITY_PERIOD_MAX_SEC = 15.0
DEFAULT_INSTANCES_TO_SKIP = 0

ListenerFn = Callable[[Literal['UP', 'DOWN'], Service], None]


class ServiceWrapper:
    service: Service
    visible_at: float | None

    __slots__ = ('service', 'visible_at')

    def __init__(self, service: Service, visible_at: float | None) -> None:
        self.service = service
        self.visible_at = visible_at

    def is_draining(self) -> bool:
        return self.service.draining

    def is_visible(self, tolerate_draining: bool = False) -> bool:
        # Skip draining nodes unless explicitly requested.
        if self.is_draining() and not tolerate_draining:
            return False

        # If `visible_at` is not set, the service is visible immediately.
        if self.visible_at is None:
            return True

        # A wrapped service with `visible_at` defined is visible when the
        # current time is *after* (greater than) `visible_at`. This is used to
        # delay visibility of a service after it has been discovered.
        is_visible = self.visible_at < time()
        if is_visible:
            # Unset `visible_at` so we don't need to check it again.
            self.visible_at = None

        return is_visible


class ServiceWatcher:
    def __init__(self, directory: Directory, key: str, ring) -> None:
        self.directory = weakref.proxy(directory)
        self.key = key

        self._lock = RLock()

        self._instances: dict[str, ServiceWrapper] = {}
        self._rotation: deque[str] = deque()
        self._ring = ring

        self._listeners: weakref.WeakSet[ListenerFn] = weakref.WeakSet()
        self._watcher_greenlet: gevent.Greenlet | None = None

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__}: {self.key}>'

    def __len__(self) -> int:
        self.ensure_watching()
        return len(self._instances)

    @property
    def ring(self):
        return self._ring

    def ring_members(
        self,
        selector,
        secondaries=DEFAULT_SECONDARIES,
    ):
        return self._ring.select(selector, secondaries)

    def select(
        self,
        selector: SelectorFunctionType | str | int | None = None,
        secondaries: int = DEFAULT_SECONDARIES,
        instances_to_skip: int = DEFAULT_INSTANCES_TO_SKIP,
        tolerate_draining: bool = False,
    ) -> Service:
        """Selects an instance of a service based on a selector."""
        self.ensure_watching()
        if selector is None or callable(selector):
            self._rotation.rotate()

            # Look in the rotation for services that match the selector, that are *also* visible.
            not_yet_visible_services = []
            for service_id in self._rotation:
                service_wrapper = self._instances[service_id]

                if not selector or selector(service_wrapper.service):
                    if service_wrapper.is_visible(tolerate_draining):
                        return service_wrapper.service
                    elif not service_wrapper.is_draining():
                        not_yet_visible_services.append(service_wrapper.service)

            # If no services are yet visible, pick a random not yet visible service and use that instead,
            # so at least we return *something* to the caller, even if it hasn't warmed up entirely yet.
            if not_yet_visible_services:
                return choice(not_yet_visible_services)

            raise ServiceUnavailable(
                f'Could not find service for {self.key!r} - {selector!r} ({secondaries!r} secondaries)'
            )
        else:
            members = self._ring.select(selector, secondaries)
            if not isinstance(members, list):
                members = [members]
            for member in members[instances_to_skip:]:
                # mypy limitations mean we can not re-use the variable name `service` in the else block
                maybe_service: ServiceWrapper | None = self._instances.get(six.ensure_str(member))
                if maybe_service and (tolerate_draining or not maybe_service.is_draining()):
                    return maybe_service.service

        raise ServiceUnavailable(
            f'Could not find service for {self.key!r} - {selector!r} ({secondaries!r} secondaries)'
        )

    def select_all(
        self,
        selector: SelectorFunctionType | None = None,
        include_not_yet_visible: bool = False,
        tolerate_draining: bool = False,
    ) -> list[Service]:
        """Selects all instances of a service based on a selector."""
        self.ensure_watching()

        not_yet_visible_services = []
        services = []
        for service_id in self._rotation:
            service_wrapper = self._instances[service_id]
            if not selector or selector(service_wrapper.service):
                if service_wrapper.is_visible(tolerate_draining) or (
                    include_not_yet_visible and not service_wrapper.is_draining()
                ):
                    services.append(service_wrapper.service)
                elif not service_wrapper.is_draining():
                    not_yet_visible_services.append(service_wrapper.service)

        # Prefer to return just the visible services if they exist, otherwise, return the not yet visible services,
        # which should be *everything else*.
        return services if services else not_yet_visible_services

    def add_lazy_listener(self, listener: ListenerFn) -> None:
        """
        Adds a function that will be notified when services come up and down.

        Note, that the listener will not begin to emit events until the service watcher begins watching. This happens
        when a method like `select` is called, or when the `ensure_watching` method is explicitly called.
        """
        self._listeners.add(listener)

    def remove_lazy_listener(self, listener: ListenerFn) -> None:
        """The inverse of `add_lazy_listener`, this method removes the given listener."""
        self._listeners.discard(listener)

    def stop(self) -> None:
        """
        Stops the service watcher. The next call to select, etc... will automatically re-start the watcher.
        """
        with self._lock:
            if self._watcher_greenlet is None:
                return

            watcher = self._watcher_greenlet
            self._watcher_greenlet = None

        watcher.kill()

    def ensure_watching(self) -> None:
        """
        Ensures that the service watcher has started. After this function returns (assuming no exceptions thrown),
        the service watcher is guaranteed to have a populated service list/ring (if enabled).
        """
        if self._watcher_greenlet is not None:
            return

        with trace('service_watcher._ensure_watching'):
            # Lock this to force anyone who needs an instance to wait for initial list.
            with self._lock:
                # If this caller was just waiting for the lock, just return.
                if self._watcher_greenlet is not None:
                    return

                watcher = self.directory.etcd_client.get_watcher(self.key, recursive=True)
                self._handle_full_sync(watcher.begin_watching(), delay_visibility=False)
                self._watcher_greenlet = gevent.spawn(self._watch, watcher)

    def _handle_full_sync(self, event: etcd.FullSyncRecursive, delay_visibility: bool = True) -> None:
        assert isinstance(event, etcd.FullSyncRecursive)

        latest_instances = {}
        for value in event.values():
            instance = Service.deserialize(value)
            latest_instances[instance.id] = instance

        # Remove any instances that no longer exist on Etcd.
        for instance_id in list(self._instances.keys()):
            if instance_id not in latest_instances:
                self._remove_instance(instance_id)

        # Add or update latest instances fetched from Etcd.
        for instance in latest_instances.values():
            self._add_instance(instance, delay_visibility)

    def _watch(self, watcher: BaseWatcher) -> None:
        try:
            for event in watcher.continue_watching():
                if isinstance(event, etcd.IncrementalSyncUpsert):
                    self._add_instance(Service.deserialize(event.value))
                elif isinstance(event, etcd.IncrementalSyncDelete):
                    self._remove_instance(Service.deserialize(event.prev_value).id)
                elif isinstance(event, etcd.FullSyncRecursive):
                    self._handle_full_sync(event)

        except gevent.GreenletExit:
            # If we are exiting because a GreenletExit was thrown, it means that `ServiceWatcher.stop` was called.
            pass

        finally:
            with self._lock:
                self._watcher_greenlet = None

    def _add_instance(self, new_instance: Service, delay_visibility: bool = True) -> None:
        new_instance_index = randint(0, len(self._rotation))
        if new_instance.id not in self._instances:
            self._rotation.insert(new_instance_index, new_instance.id)
            self._instances[new_instance.id] = ServiceWrapper(
                service=new_instance, visible_at=random_visibility_period() if delay_visibility else None
            )
            for listener in self._listeners:
                gevent.spawn(listener, UP, new_instance)

            log.debug('discovery: + %s@%s', new_instance.name, new_instance.id)
        else:
            existing_instance = self._instances[new_instance.id]
            existing_instance.service.merge(new_instance)
            if existing_instance.service.id not in self._rotation:
                self._rotation.insert(new_instance_index, new_instance.id)

    def _remove_instance(self, instance_id: str) -> None:
        old_instance = self._instances.pop(instance_id, None)
        if old_instance is None:
            return

        old_instance_service = old_instance.service

        self._rotation.remove(old_instance_service.id)
        for listener in self._listeners:
            gevent.spawn(listener, DOWN, old_instance_service)

        log.debug('discovery: - %s@%s', old_instance_service.name, old_instance_service.id)


def random_visibility_period() -> float:
    """Computes when the service should be considered as being "in rotation". This is a random interval in order
    to prevent thundering herding connections to a service that has just come online."""
    return time() + uniform(0, VISIBLITY_PERIOD_MAX_SEC)
