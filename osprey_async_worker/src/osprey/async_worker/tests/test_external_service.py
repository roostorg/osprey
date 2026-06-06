"""Tests for the async external service cache."""

import asyncio
from datetime import timedelta
from typing import Optional, Sequence

import pytest
from result import Err, Ok, Result

from osprey.async_worker.lib.external_service import AsyncExternalService, ExternalServiceAccessor


class FakeService(AsyncExternalService[str, str]):
    """Test service that records calls and returns predictable results."""

    def __init__(self, delay: float = 0.0):
        self.call_count = 0
        self.delay = delay

    async def get_from_service(self, key: str) -> str:
        self.call_count += 1
        if self.delay > 0:
            await asyncio.sleep(self.delay)
        return f'value_{key}'


class FailingService(AsyncExternalService[str, str]):
    """Test service that always raises."""

    async def get_from_service(self, key: str) -> str:
        raise ValueError(f'service error for {key}')


class FailOnceService(AsyncExternalService[str, Optional[str]]):
    """Raises on first call per key, succeeds after."""

    def __init__(self):
        self.seen = set()

    def count_error_once(self) -> bool:
        return True

    async def get_from_service(self, key: str) -> Optional[str]:
        if key not in self.seen:
            self.seen.add(key)
            raise ValueError('first call fails')
        return f'value_{key}'


class TTLService(AsyncExternalService[str, str]):
    def __init__(self, ttl: timedelta):
        self._ttl = ttl
        self.call_count = 0

    def cache_ttl(self) -> Optional[timedelta]:
        return self._ttl

    async def get_from_service(self, key: str) -> str:
        self.call_count += 1
        return f'value_{key}_{self.call_count}'


class BatchService(AsyncExternalService[str, str]):
    """Test service that supports batch operations."""

    def __init__(self):
        self.batch_call_count = 0

    async def get_from_service(self, key: str) -> str:
        return f'value_{key}'

    async def batch_get_from_service(self, keys: Sequence[str]) -> Sequence[Result[str, Exception]]:
        self.batch_call_count += 1
        return [Ok(f'batch_{key}') for key in keys]


# --- Cache tests ---


@pytest.mark.asyncio
async def test_get_returns_value():
    service = FakeService()
    accessor = ExternalServiceAccessor(service)
    result = await accessor.get('foo')
    assert result == 'value_foo'


@pytest.mark.asyncio
async def test_get_caches_result():
    service = FakeService()
    accessor = ExternalServiceAccessor(service)
    await accessor.get('foo')
    await accessor.get('foo')
    assert service.call_count == 1


@pytest.mark.asyncio
async def test_get_different_keys_not_cached():
    service = FakeService()
    accessor = ExternalServiceAccessor(service)
    await accessor.get('foo')
    await accessor.get('bar')
    assert service.call_count == 2


@pytest.mark.asyncio
async def test_get_without_cache_bypasses():
    service = FakeService()
    accessor = ExternalServiceAccessor(service)
    await accessor.get('foo')
    await accessor.get_without_cache('foo')
    assert service.call_count == 2


@pytest.mark.asyncio
async def test_get_without_cache_updates_cache():
    service = FakeService()
    accessor = ExternalServiceAccessor(service)
    await accessor.get_without_cache('foo')
    await accessor.get('foo')
    assert service.call_count == 1  # Second get hits cache


# --- Concurrent access (future dedup) ---


@pytest.mark.asyncio
async def test_concurrent_get_deduplicates():
    """Multiple concurrent gets for the same key should only call service once."""
    service = FakeService(delay=0.05)
    accessor = ExternalServiceAccessor(service)
    results = await asyncio.gather(
        accessor.get('foo'),
        accessor.get('foo'),
        accessor.get('foo'),
    )
    assert all(r == 'value_foo' for r in results)
    assert service.call_count == 1


# --- Error handling ---


@pytest.mark.asyncio
async def test_get_propagates_error():
    service = FailingService()
    accessor = ExternalServiceAccessor(service)
    with pytest.raises(ValueError, match='service error for foo'):
        await accessor.get('foo')


@pytest.mark.asyncio
async def test_get_error_cached():
    """Errors are cached — second get raises the same error."""
    service = FailingService()
    accessor = ExternalServiceAccessor(service)
    with pytest.raises(ValueError):
        await accessor.get('foo')
    with pytest.raises(ValueError):
        await accessor.get('foo')


@pytest.mark.asyncio
async def test_count_error_once():
    """With count_error_once, subsequent callers get None instead of the error."""
    service = FailOnceService()
    accessor = ExternalServiceAccessor(service)
    with pytest.raises(ValueError):
        await accessor.get('foo')
    # Second get should return None (cached as None due to count_error_once)
    result = await accessor.get('foo')
    assert result is None


# --- TTL ---


@pytest.mark.asyncio
async def test_ttl_expires_cache():
    """Expired TTL causes a re-fetch."""
    service = TTLService(ttl=timedelta(days=-1))  # Immediately expired
    accessor = ExternalServiceAccessor(service)
    r1 = await accessor.get('foo')
    r2 = await accessor.get('foo')
    assert r1 != r2  # Different values = two service calls
    assert service.call_count == 2


@pytest.mark.asyncio
async def test_no_ttl_caches_forever():
    service = FakeService()
    accessor = ExternalServiceAccessor(service)
    await accessor.get('foo')
    await accessor.get('foo')
    await accessor.get('foo')
    assert service.call_count == 1


# --- Batch ---


@pytest.mark.asyncio
async def test_batch_get():
    service = BatchService()
    accessor = ExternalServiceAccessor(service)
    results = await accessor.batch_get(['a', 'b', 'c'])
    assert len(results) == 3
    assert results[0] == Ok('batch_a')
    assert results[1] == Ok('batch_b')
    assert results[2] == Ok('batch_c')
    assert service.batch_call_count == 1


@pytest.mark.asyncio
async def test_batch_get_uses_cache():
    service = BatchService()
    accessor = ExternalServiceAccessor(service)
    await accessor.batch_get(['a', 'b'])
    # Second batch with overlap — 'a' and 'b' cached, only 'c' fetched
    results = await accessor.batch_get(['a', 'b', 'c'])
    assert len(results) == 3
    assert service.batch_call_count == 2
