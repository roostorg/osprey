from typing import List, Optional

import gevent
import pytest
from gevent.event import Event
from osprey.engine.executor.external_service_utils import ExternalService, ExternalServiceAccessor


class CountingService(ExternalService[str, int]):
    def __init__(self) -> None:
        from typing import List

        self.calls: List[str] = []

    def get_from_service(self, key: str) -> int:
        self.calls.append(key)
        return len(self.calls)


class BlockingService(CountingService):
    def __init__(self) -> None:
        super().__init__()
        self.blocking_events: List[Event] = []

    def get_from_service(self, key: str) -> int:
        event = Event()
        self.blocking_events.append(event)
        event.wait()
        self.blocking_events.remove(event)

        return super().get_from_service(key)


class FailingService(ExternalService[str, int]):
    """Always raises on the first call for a key."""

    def __init__(self) -> None:
        self.calls: List[str] = []

    def get_from_service(self, key: str) -> int:
        self.calls.append(key)
        raise RuntimeError(f'timeout for {key}')


class SuppressedFailingService(ExternalService[str, Optional[int]]):
    """Always raises, but opts in to suppress_cached_errors."""

    def __init__(self) -> None:
        self.calls: List[str] = []

    def suppress_cached_errors(self) -> bool:
        return True

    def get_from_service(self, key: str) -> Optional[int]:
        self.calls.append(key)
        raise RuntimeError(f'timeout for {key}')


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


def test_cached_errors_re_raise_by_default() -> None:
    """Without suppress_cached_errors, cached exceptions re-raise for all callers."""
    service = FailingService()
    accessor = ExternalServiceAccessor(service)

    with pytest.raises(RuntimeError, match='timeout for a'):
        accessor.get('a')

    # Subsequent call also raises from cache
    with pytest.raises(RuntimeError, match='timeout for a'):
        accessor.get('a')

    # Only one service call was made
    assert service.calls == ['a']


def test_suppress_cached_errors_returns_none_on_subsequent_calls() -> None:
    """With suppress_cached_errors, the first caller gets the exception but
    subsequent callers receive None from the cache."""
    service = SuppressedFailingService()
    accessor = ExternalServiceAccessor(service)

    with pytest.raises(RuntimeError, match='timeout for a'):
        accessor.get('a')

    # Subsequent call returns None from cache
    assert accessor.get('a') is None

    # Only one service call was made
    assert service.calls == ['a']


def test_suppress_cached_errors_concurrent_waiters() -> None:
    """With suppress_cached_errors, concurrent waiters get None while the
    initiating greenlet gets the exception."""

    class BlockingThenFailService(ExternalService[str, Optional[int]]):
        def __init__(self) -> None:
            self.event = Event()

        def suppress_cached_errors(self) -> bool:
            return True

        def get_from_service(self, key: str) -> Optional[int]:
            self.event.wait()
            raise RuntimeError('timeout')

    service = BlockingThenFailService()
    accessor = ExternalServiceAccessor(service)

    g1 = gevent.spawn(lambda: accessor.get('a'))
    g2 = gevent.spawn(lambda: accessor.get('a'))
    gevent.idle()

    service.event.set()
    gevent.idle()

    # The initiating greenlet gets the exception
    assert g1.exception is not None or g2.exception is not None

    # The waiting greenlet gets None
    results = [g.value for g in [g1, g2] if g.exception is None]
    assert None in results
