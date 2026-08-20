"""Tests for the async external service cache."""

import asyncio
import gc
from collections.abc import Sequence
from datetime import timedelta

import pytest
from osprey.async_worker.lib.external_service import AsyncExternalService, ExternalServiceAccessor
from result import Err, Ok, Result


class FakeService(AsyncExternalService[str, str]):
    def __init__(self, delay: float = 0.0):
        self.call_count = 0
        self.delay = delay

    async def get_from_service(self, key: str) -> str:
        self.call_count += 1
        if self.delay > 0:
            await asyncio.sleep(self.delay)
        return f'value_{key}'


class FailingService(AsyncExternalService[str, str]):
    def __init__(self):
        self.call_count = 0

    async def get_from_service(self, key: str) -> str:
        self.call_count += 1
        raise ValueError(f'service error for {key}')


class CountErrorOnceGatedService(AsyncExternalService[str, str | None]):
    def __init__(self):
        self.call_count = 0
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    def count_error_once(self) -> bool:
        return True

    async def get_from_service(self, key: str) -> str | None:
        self.call_count += 1
        self.started.set()
        await self.release.wait()
        raise ValueError('service fails')


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

    def cache_ttl(self) -> timedelta | None:
        return self._ttl

    async def get_from_service(self, key: str) -> str:
        self.call_count += 1
        return f'value_{key}_{self.call_count}'


class BatchService(AsyncExternalService[str, str]):
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


class CountErrorOnceBatchService(AsyncExternalService[str, str | None]):
    def count_error_once(self) -> bool:
        return True

    async def get_from_service(self, key: str) -> str | None:
        raise ValueError('single read fails')

    async def batch_get_from_service(self, keys: Sequence[str]) -> Sequence[Result[str | None, Exception]]:
        return [Err(ValueError('batch read fails')) for _ in keys]


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


class FailingGatedBatchService(AsyncExternalService[str, str]):
    def __init__(self):
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def get_from_service(self, key: str) -> str:
        raise ValueError('single read fails')

    async def batch_get_from_service(self, keys: Sequence[str]) -> Sequence[Result[str, Exception]]:
        self.started.set()
        await self.release.wait()
        raise ValueError('batch fails')


@pytest.mark.asyncio
async def test_get_returns_value():
    accessor = ExternalServiceAccessor(FakeService())
    assert await accessor.get('foo') == 'value_foo'


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
async def test_get_without_cache_updates_cache():
    service = FakeService()
    accessor = ExternalServiceAccessor(service)
    await accessor.get_without_cache('foo')
    await accessor.get('foo')
    assert service.call_count == 1


@pytest.mark.asyncio
async def test_cancelling_get_without_cache_does_not_cancel_shared_get():
    service = CancelOnceService()
    accessor = ExternalServiceAccessor(service)
    owner = asyncio.create_task(accessor.get_without_cache('foo'))
    await asyncio.wait_for(service.started.wait(), timeout=1)

    owner.cancel()
    with pytest.raises(asyncio.CancelledError):
        _ = await owner
    service.release.set()

    assert await accessor.get('foo') == 'value_foo'
    assert service.call_count == 1


@pytest.mark.asyncio
async def test_concurrent_get_deduplicates():
    service = FakeService(delay=0.05)
    accessor = ExternalServiceAccessor(service)
    results = await asyncio.gather(accessor.get('foo'), accessor.get('foo'), accessor.get('foo'))
    assert results == ['value_foo', 'value_foo', 'value_foo']
    assert service.call_count == 1


@pytest.mark.asyncio
async def test_cancelling_waiter_does_not_cancel_shared_get():
    service = CancelOnceService()
    accessor = ExternalServiceAccessor(service)
    owner = asyncio.create_task(accessor.get('foo'))
    await asyncio.wait_for(service.started.wait(), timeout=1)
    waiter = asyncio.create_task(accessor.get('foo'))

    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        _ = await waiter

    service.release.set()
    assert await owner == 'value_foo'
    assert service.call_count == 1


@pytest.mark.asyncio
async def test_cancelling_owner_does_not_cancel_shared_get():
    service = CancelOnceService()
    accessor = ExternalServiceAccessor(service)
    owner = asyncio.create_task(accessor.get('foo'))
    await asyncio.wait_for(service.started.wait(), timeout=1)
    survivor = asyncio.create_task(accessor.get('foo'))

    owner.cancel()
    with pytest.raises(asyncio.CancelledError):
        _ = await owner

    service.release.set()
    assert await survivor == 'value_foo'
    assert service.call_count == 1


@pytest.mark.asyncio
async def test_get_propagates_error():
    service = FailingService()
    accessor = ExternalServiceAccessor(service)
    with pytest.raises(ValueError, match='service error for foo'):
        await accessor.get('foo')
    assert service.call_count == 1


@pytest.mark.asyncio
async def test_get_caches_failed_request():
    service = FailingService()
    accessor = ExternalServiceAccessor(service)
    with pytest.raises(ValueError, match='service error for foo'):
        await accessor.get('foo')
    with pytest.raises(ValueError, match='service error for foo'):
        await accessor.get('foo')
    assert service.call_count == 1


@pytest.mark.asyncio
async def test_failed_task_does_not_evict_replacement():
    service = ReplacementService()
    accessor = ExternalServiceAccessor(service)
    first = asyncio.create_task(accessor.get('foo'))
    await asyncio.wait_for(service.first_started.wait(), timeout=1)
    replacement = asyncio.create_task(accessor.get_without_cache('foo'))
    await asyncio.wait_for(service.second_started.wait(), timeout=1)

    service.first_release.set()
    with pytest.raises(ValueError):
        _ = await first
    service.second_release.set()

    assert await replacement == 'value_foo'
    assert await accessor.get('foo') == 'value_foo'
    assert service.call_count == 2


@pytest.mark.asyncio
async def test_count_error_once_with_concurrent_waiter():
    service = CountErrorOnceGatedService()
    accessor = ExternalServiceAccessor(service)
    creator = asyncio.create_task(accessor.get('foo'))
    await asyncio.wait_for(service.started.wait(), timeout=1)
    waiter_started = asyncio.Event()

    async def wait_for_cached_get():
        # Do not suspend before the cached future is attached below
        waiter_started.set()
        return await accessor.get('foo')

    waiter = asyncio.create_task(wait_for_cached_get())
    await asyncio.wait_for(waiter_started.wait(), timeout=1)
    service.release.set()

    with pytest.raises(ValueError):
        _ = await creator
    assert await waiter is None
    assert await accessor.get('foo') is None
    assert service.call_count == 1


@pytest.mark.asyncio
async def test_count_error_once_does_not_apply_to_get_without_cache():
    service = CountErrorOnceGatedService()
    accessor = ExternalServiceAccessor(service)
    creator = asyncio.create_task(accessor.get_without_cache('foo'))
    await asyncio.wait_for(service.started.wait(), timeout=1)
    service.release.set()

    with pytest.raises(ValueError, match='service fails'):
        _ = await creator
    with pytest.raises(ValueError, match='service fails'):
        _ = await accessor.get('foo')
    assert service.call_count == 1


@pytest.mark.asyncio
async def test_ttl_expires_cache():
    service = TTLService(ttl=timedelta(days=-1))
    accessor = ExternalServiceAccessor(service)
    first = await accessor.get('foo')
    second = await accessor.get('foo')
    assert first != second
    assert service.call_count == 2


@pytest.mark.asyncio
async def test_no_ttl_caches_forever():
    service = FakeService()
    accessor = ExternalServiceAccessor(service)
    await accessor.get('foo')
    await accessor.get('foo')
    await accessor.get('foo')
    assert service.call_count == 1


@pytest.mark.asyncio
async def test_batch_get_returns_values_and_uses_cache():
    service = BatchService()
    accessor = ExternalServiceAccessor(service)
    assert await accessor.batch_get(['a', 'b', 'c']) == [Ok('batch_a'), Ok('batch_b'), Ok('batch_c')]
    assert await accessor.batch_get(['a', 'b', 'c']) == [Ok('batch_a'), Ok('batch_b'), Ok('batch_c')]
    assert service.batch_call_count == 1


@pytest.mark.asyncio
async def test_batch_get_deduplicates_duplicate_keys():
    service = BatchService()
    accessor = ExternalServiceAccessor(service)
    assert await accessor.batch_get(['a', 'a']) == [Ok('batch_a'), Ok('batch_a')]
    assert service.batch_call_count == 1


@pytest.mark.asyncio
async def test_batch_get_resolves_its_owned_future_after_cache_replacement():
    service = GatedBatchService()
    accessor = ExternalServiceAccessor(service)
    batch_get = asyncio.create_task(accessor.batch_get(['a']))
    await asyncio.wait_for(service.started.wait(), timeout=1)

    assert await accessor.get_without_cache('a') == 'value_a'
    service.release.set()

    assert await asyncio.wait_for(batch_get, timeout=1) == [Ok('batch_a')]
    assert await accessor.get('a') == 'value_a'


@pytest.mark.asyncio
async def test_cancelled_batch_loader_evicts_its_cache_entries():
    service = GatedBatchService()
    accessor = ExternalServiceAccessor(service)
    batch = asyncio.create_task(accessor.batch_get(['a']))
    await asyncio.wait_for(service.started.wait(), timeout=1)

    loader = next(iter(accessor._active_batch_loaders))
    loader.cancel()
    with pytest.raises(asyncio.CancelledError):
        _ = await batch
    service.release.set()

    assert await accessor.batch_get(['a']) == [Ok('batch_a')]
    assert service.batch_call_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize('raise_exception', [False, True])
async def test_batch_error_is_cached(raise_exception: bool):
    service = FailOnceBatchService(raise_exception)
    accessor = ExternalServiceAccessor(service)
    first = await accessor.batch_get(['a'])
    second = await accessor.batch_get(['a'])
    assert first[0].is_err()
    assert second[0].is_err()
    assert service.batch_call_count == 1


@pytest.mark.asyncio
async def test_count_error_once_does_not_apply_to_batch_failure():
    accessor = ExternalServiceAccessor(CountErrorOnceBatchService())
    batch_result = await accessor.batch_get(['a'])

    assert batch_result[0].is_err()
    with pytest.raises(ValueError, match='batch read fails'):
        await accessor.get('a')


@pytest.mark.asyncio
async def test_cancelling_batch_owner_does_not_cancel_shared_get():
    service = GatedBatchService()
    accessor = ExternalServiceAccessor(service)
    batch = asyncio.create_task(accessor.batch_get(['a']))
    await asyncio.wait_for(service.started.wait(), timeout=1)
    survivor = asyncio.create_task(accessor.get('a'))

    batch.cancel()
    with pytest.raises(asyncio.CancelledError):
        _ = await batch
    service.release.set()

    assert await survivor == 'batch_a'
    assert service.batch_call_count == 1
    assert service.get_call_count == 0


@pytest.mark.asyncio
async def test_cancelled_batch_owner_keeps_loader_alive_through_garbage_collection():
    service = GatedBatchService()
    accessor = ExternalServiceAccessor(service)
    batch = asyncio.create_task(accessor.batch_get(['a']))
    await asyncio.wait_for(service.started.wait(), timeout=1)
    survivor = asyncio.create_task(accessor.get('a'))

    batch.cancel()
    with pytest.raises(asyncio.CancelledError):
        _ = await batch
    gc.collect()
    service.release.set()

    assert await survivor == 'batch_a'
    assert service.batch_call_count == 1


@pytest.mark.asyncio
async def test_cancelled_failed_batch_consumes_future_exceptions():
    service = FailingGatedBatchService()
    accessor = ExternalServiceAccessor(service)
    loop = asyncio.get_running_loop()
    contexts: list[dict[str, object]] = []
    previous_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: contexts.append(context))
    try:
        batch = asyncio.create_task(accessor.batch_get(['a']))
        await asyncio.wait_for(service.started.wait(), timeout=1)
        loader = next(iter(accessor._active_batch_loaders))

        batch.cancel()
        with pytest.raises(asyncio.CancelledError):
            _ = await batch
        service.release.set()
        await asyncio.wait_for(asyncio.shield(loader), timeout=1)
        accessor._cache.clear()
        gc.collect()
        await asyncio.sleep(0)
    finally:
        loop.set_exception_handler(previous_handler)

    assert not any(context.get('message') == 'Future exception was never retrieved' for context in contexts)


@pytest.mark.asyncio
async def test_cancelling_batch_waiter_does_not_cancel_shared_get():
    service = CancelOnceService()
    accessor = ExternalServiceAccessor(service)
    owner = asyncio.create_task(accessor.get('a'))
    await asyncio.wait_for(service.started.wait(), timeout=1)
    batch_started = asyncio.Event()

    async def wait_for_cached_batch():
        # Do not suspend before the cached future is attached below
        batch_started.set()
        return await accessor.batch_get(['a'])

    batch = asyncio.create_task(wait_for_cached_batch())
    await asyncio.wait_for(batch_started.wait(), timeout=1)

    batch.cancel()
    with pytest.raises(asyncio.CancelledError):
        _ = await batch
    service.release.set()

    assert await owner == 'value_a'
    assert service.call_count == 1
