"""Tests for the async external service cache."""

import asyncio
from datetime import timedelta
from typing import Optional, Sequence

import pytest
from osprey.async_worker.lib.external_service import AsyncExternalService, ExternalServiceAccessor
from result import Err, Ok, Result


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


class CountErrorOnceGatedService(AsyncExternalService[str, Optional[str]]):
    def __init__(self):
        self.call_count = 0
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    def count_error_once(self) -> bool:
        return True

    async def get_from_service(self, key: str) -> Optional[str]:
        self.call_count += 1
        self.started.set()
        await self.release.wait()
        raise ValueError('service fails')


class FailOnceGatedService(AsyncExternalService[str, str]):
    def __init__(self):
        self.call_count = 0
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def get_from_service(self, key: str) -> str:
        self.call_count += 1
        if self.call_count == 1:
            self.started.set()
            await self.release.wait()
            raise ValueError('first call fails')
        return f'value_{key}'


class CancelOnceService(AsyncExternalService[str, str]):
    def __init__(self):
        self.call_count = 0
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def get_from_service(self, key: str) -> str:
        self.call_count += 1
        if self.call_count == 1:
            self.started.set()
            await self.release.wait()
        return f'value_{key}'


class ReplacementService(AsyncExternalService[str, str]):
    def __init__(self):
        self.call_count = 0
        self.first_started = asyncio.Event()
        self.first_release = asyncio.Event()
        self.second_started = asyncio.Event()
        self.second_release = asyncio.Event()

    async def get_from_service(self, key: str) -> str:
        self.call_count += 1
        if self.call_count == 1:
            self.first_started.set()
            await self.first_release.wait()
            raise ValueError('first call fails')
        if self.call_count == 2:
            self.second_started.set()
            await self.second_release.wait()
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


class FailOnceBatchService(AsyncExternalService[str, str]):
    def __init__(self, raise_exception: bool):
        self.raise_exception = raise_exception
        self.batch_call_count = 0

    async def get_from_service(self, key: str) -> str:
        return f'value_{key}'

    async def batch_get_from_service(self, keys: Sequence[str]) -> Sequence[Result[str, Exception]]:
        self.batch_call_count += 1
        if self.batch_call_count == 1:
            if self.raise_exception:
                raise ValueError('batch fails')
            return [Err(ValueError('item fails')) for _ in keys]
        return [Ok(f'batch_{key}') for key in keys]


class GatedBatchService(AsyncExternalService[str, str]):
    def __init__(self):
        self.get_call_count = 0
        self.batch_call_count = 0
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def get_from_service(self, key: str) -> str:
        self.get_call_count += 1
        return f'value_{key}'

    async def batch_get_from_service(self, keys: Sequence[str]) -> Sequence[Result[str, Exception]]:
        self.batch_call_count += 1
        self.started.set()
        await self.release.wait()
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


@pytest.mark.asyncio
async def test_cancelling_waiter_does_not_cancel_shared_get():
    service = CancelOnceService()
    accessor = ExternalServiceAccessor(service)
    owner = asyncio.create_task(accessor.get('foo'))
    await service.started.wait()
    waiter = asyncio.create_task(accessor.get('foo'))

    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter

    service.release.set()
    assert await owner == 'value_foo'
    assert service.call_count == 1


@pytest.mark.asyncio
async def test_cancelling_owner_does_not_cancel_shared_get():
    service = CancelOnceService()
    accessor = ExternalServiceAccessor(service)
    owner = asyncio.create_task(accessor.get('foo'))
    await service.started.wait()
    survivor = asyncio.create_task(accessor.get('foo'))

    owner.cancel()
    with pytest.raises(asyncio.CancelledError):
        await owner

    service.release.set()
    assert await survivor == 'value_foo'
    assert service.call_count == 1


# --- Error handling ---


@pytest.mark.asyncio
async def test_get_propagates_error():
    service = FailingService()
    accessor = ExternalServiceAccessor(service)
    with pytest.raises(ValueError, match='service error for foo'):
        await accessor.get('foo')


@pytest.mark.asyncio
async def test_concurrent_error_is_shared_then_retried():
    service = FailOnceGatedService()
    accessor = ExternalServiceAccessor(service)
    callers = [asyncio.create_task(accessor.get('foo')) for _ in range(3)]
    await service.started.wait()
    await asyncio.sleep(0)
    service.release.set()

    results = await asyncio.gather(*callers, return_exceptions=True)
    assert all(isinstance(result, ValueError) for result in results)
    assert await accessor.get('foo') == 'value_foo'
    assert service.call_count == 2


@pytest.mark.asyncio
async def test_failed_task_does_not_evict_replacement():
    service = ReplacementService()
    accessor = ExternalServiceAccessor(service)
    first = asyncio.create_task(accessor.get('foo'))
    await service.first_started.wait()
    replacement = asyncio.create_task(accessor.get_without_cache('foo'))
    await service.second_started.wait()

    service.first_release.set()
    with pytest.raises(ValueError):
        await first
    service.second_release.set()

    assert await replacement == 'value_foo'
    assert await accessor.get('foo') == 'value_foo'
    assert service.call_count == 2


@pytest.mark.asyncio
async def test_count_error_once_with_concurrent_waiter():
    service = CountErrorOnceGatedService()
    accessor = ExternalServiceAccessor(service)
    creator = asyncio.create_task(accessor.get('foo'))
    await service.started.wait()
    waiter = asyncio.create_task(accessor.get('foo'))
    await asyncio.sleep(0)
    service.release.set()

    with pytest.raises(ValueError):
        await creator
    assert await waiter is None
    assert await accessor.get('foo') is None
    assert service.call_count == 1


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


@pytest.mark.asyncio
@pytest.mark.parametrize('raise_exception', [False, True])
async def test_batch_error_is_not_cached(raise_exception: bool):
    service = FailOnceBatchService(raise_exception)
    accessor = ExternalServiceAccessor(service)

    first = await accessor.batch_get(['a'])
    second = await accessor.batch_get(['a'])

    assert first[0].is_err()
    assert second == [Ok('batch_a')]
    assert service.batch_call_count == 2


@pytest.mark.asyncio
async def test_cancelling_batch_owner_does_not_cancel_shared_get():
    service = GatedBatchService()
    accessor = ExternalServiceAccessor(service)
    batch = asyncio.create_task(accessor.batch_get(['a']))
    await service.started.wait()
    survivor = asyncio.create_task(accessor.get('a'))

    batch.cancel()
    with pytest.raises(asyncio.CancelledError):
        await batch
    service.release.set()

    assert await survivor == 'batch_a'
    assert service.batch_call_count == 1
    assert service.get_call_count == 0


@pytest.mark.asyncio
async def test_cancelling_batch_waiter_does_not_cancel_shared_get():
    service = CancelOnceService()
    accessor = ExternalServiceAccessor(service)
    owner = asyncio.create_task(accessor.get('a'))
    await service.started.wait()
    batch = asyncio.create_task(accessor.batch_get(['a']))
    await asyncio.sleep(0)

    batch.cancel()
    with pytest.raises(asyncio.CancelledError):
        await batch
    service.release.set()

    assert await owner == 'value_a'
    assert service.call_count == 1
