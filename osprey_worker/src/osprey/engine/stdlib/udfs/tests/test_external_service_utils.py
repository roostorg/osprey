import gevent
from gevent.event import Event
from osprey.engine.executor.external_service_utils import ExternalService, ExternalServiceAccessor


class CountingService(ExternalService[str, int]):
    def __init__(self) -> None:
        self.calls: list[str] = []

    def get_from_service(self, key: str) -> int:
        self.calls.append(key)
        return len(self.calls)


class BlockingService(CountingService):
    def __init__(self) -> None:
        super().__init__()
        self.blocking_events: list[Event] = []

    def get_from_service(self, key: str) -> int:
        event = Event()
        self.blocking_events.append(event)
        event.wait()
        self.blocking_events.remove(event)

        return super().get_from_service(key)


def test_accessor_caches_values() -> None:
    service = CountingService()
    accessor = ExternalServiceAccessor(service)

    assert accessor.get('a') == 1
    assert accessor.get('a') == 1
    assert service.calls == ['a']

    assert accessor.get('b') == 2
    assert service.calls == ['a', 'b']


def test_will_debounce_multiple_requests() -> None:
    service = BlockingService()
    accessor = ExternalServiceAccessor(service)

    g1 = gevent.spawn(lambda: accessor.get('a'))
    g2 = gevent.spawn(lambda: accessor.get('a'))
    gevent.idle()

    assert len(service.blocking_events) == 1
    service.blocking_events[0].set()
    gevent.idle()

    assert service.blocking_events == []
    assert service.calls == ['a']
    assert g1.get() == 1
    assert g2.get() == 1

    # Do it again after above resolved
    g1 = gevent.spawn(lambda: accessor.get('a'))
    g2 = gevent.spawn(lambda: accessor.get('a'))
    gevent.idle()

    assert service.blocking_events == []
    assert service.calls == ['a']
    assert g1.get() == 1
    assert g2.get() == 1


def test_can_request_different_keys_in_parallel() -> None:
    service = BlockingService()
    accessor = ExternalServiceAccessor(service)

    g1 = gevent.spawn(lambda: accessor.get('a'))
    g2 = gevent.spawn(lambda: accessor.get('b'))
    g3 = gevent.spawn(lambda: accessor.get('a'))
    gevent.idle()

    assert len(service.blocking_events) == 2
    service.blocking_events[0].set()
    service.blocking_events[1].set()
    gevent.idle()

    assert service.blocking_events == []
    assert service.calls == ['a', 'b']
    assert g1.get() == 1
    assert g2.get() == 2
    assert g3.get() == 1
