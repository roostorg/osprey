"""Async external service utilities for the async worker.

Port of osprey.engine.executor.external_service_utils with asyncio instead of gevent.
Uses asyncio.Task instead of gevent.event.AsyncResult for single-value cache entries.
"""

import asyncio
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Dict, Generic, Hashable, Optional, Sequence, Tuple, TypeVar, cast

from result import Err, Ok, Result

KeyT = TypeVar('KeyT', bound=Hashable)
ValueT = TypeVar('ValueT')


class AsyncExternalService(ABC, Generic[KeyT, ValueT]):
    @abstractmethod
    async def get_from_service(self, key: KeyT) -> ValueT:
        raise NotImplementedError

    # Not abstract because not all services support batching multiple keys
    async def batch_get_from_service(self, keys: Sequence[KeyT]) -> Sequence[Result[ValueT, Exception]]:
        raise NotImplementedError

    def cache_ttl(self) -> Optional[timedelta]:
        """
        Returns a time to live for items in the cache. By default, KVs are cached indefinitely.

        To have cache entries auto-expire, override this method in your external service definition.

        Note that timedeltas can accept negative values to represent the past, but only on the days field.
        You *can* use timedelta(seconds=0) to disable caching, but a negative time delta *ensures* that even
        if a time shift occurs (such as daylight savings), the cache_ttl will still be immediate.

        Therefore, to disable the read cache, it is recommended to set this to `timedelta(days=-1)`
        """
        return None

    def count_error_once(self) -> bool:
        """
        When True, only the caller that initiated the external service call
        receives the exception. Subsequent callers that would hit the cached
        error receive None instead.

        Only enable this when ValueT is Optional and None is a safe fallback.
        """
        return False


class ExternalServiceAccessor(Generic[KeyT, ValueT]):
    """Facilitates accessing an async external service in a way that caches and debounces requests based on a key."""

    def __init__(self, service: AsyncExternalService[KeyT, ValueT]):
        self._service = service
        # Key -> Tuple[ Future[ValueT], Expiration datetime ]
        self._cache: Dict[KeyT, Tuple[asyncio.Future[ValueT], Optional[datetime]]] = {}

    def _is_past_cache_expiration(self, cache_expiration: Optional[datetime]) -> bool:
        """
        Helper method to perform a time check on an optional datetime.
        """
        if cache_expiration is None:
            return False
        return datetime.now() > cache_expiration

    def _get_cache_expiration_datetime(self) -> Optional[datetime]:
        """
        Helper method to generate an optional cache expiration datetime based on the cache TTL.
        """
        ttl = self._service.cache_ttl()
        return datetime.now() + ttl if ttl is not None else None

    def _make_task(self, key: KeyT) -> asyncio.Task[ValueT]:
        task = asyncio.create_task(self._service.get_from_service(key))
        task.add_done_callback(lambda completed: self._evict_failed_entry(key, completed))
        return task

    def _make_future(self, key: KeyT) -> asyncio.Future[ValueT]:
        future: asyncio.Future[ValueT] = asyncio.get_running_loop().create_future()
        future.add_done_callback(lambda completed: self._evict_failed_entry(key, completed))
        return future

    def _evict_failed_entry(self, key: KeyT, future: asyncio.Future[ValueT]) -> None:
        failed = future.cancelled() or future.exception() is not None
        cache_entry = self._cache.get(key)
        if failed and not self._service.count_error_once() and cache_entry is not None and cache_entry[0] is future:
            del self._cache[key]

    async def _load_batch(self, keys: Sequence[KeyT], futures: Sequence[asyncio.Future[ValueT]]) -> None:
        try:
            results = await self._service.batch_get_from_service(keys)
            if len(results) != len(keys):
                raise ValueError(f'batch service returned {len(results)} results for {len(keys)} keys')
            for future, result in zip(futures, results):
                if result.is_ok():
                    future.set_result(cast(ValueT, result.value))
                else:
                    future.set_exception(cast(BaseException, result.value))
        except asyncio.CancelledError:
            for future in futures:
                if not future.done():
                    future.cancel()
            raise
        except Exception as error:
            for future in futures:
                if not future.done():
                    future.set_exception(error)

    async def get_without_cache(self, key: KeyT) -> ValueT:
        """
        Ignores any cached values and performs a read-through `get` to the external service.
        The new value is then used to update the cache entry for subsequent `get` calls.
        """
        task = self._make_task(key)
        cache_entry: Tuple[asyncio.Future[ValueT], Optional[datetime]] = (
            task,
            self._get_cache_expiration_datetime(),
        )
        self._cache[key] = cache_entry
        return await asyncio.shield(task)

    async def get(self, key: KeyT) -> ValueT:
        cache_entry = self._cache.get(key)
        is_creator = cache_entry is None or self._is_past_cache_expiration(cache_entry[1])
        if is_creator:
            task = self._make_task(key)
            cache_entry = (task, self._get_cache_expiration_datetime())
            self._cache[key] = cache_entry
        assert cache_entry is not None
        try:
            return await asyncio.shield(cache_entry[0])
        except Exception:
            if self._service.count_error_once() and not is_creator:
                return cast(ValueT, None)
            raise

    async def batch_get(self, keys: Sequence[KeyT]) -> Sequence[Result[ValueT, Exception]]:
        non_cached_keys = []
        for key in dict.fromkeys(keys):
            cache_entry = self._cache.get(key)
            if cache_entry is None or self._is_past_cache_expiration(cache_entry[1]):
                non_cached_keys.append(key)
        if non_cached_keys:
            futures = []
            for key in non_cached_keys:
                future = self._make_future(key)
                self._cache[key] = (future, self._get_cache_expiration_datetime())
                futures.append(future)
        futures_by_key = {key: self._cache[key][0] for key in keys}
        if non_cached_keys:
            loader = asyncio.create_task(self._load_batch(non_cached_keys, futures))
            await asyncio.shield(loader)

        results: list[Result[ValueT, Exception]] = []
        for key in keys:
            try:
                value = await asyncio.shield(futures_by_key[key])
                results.append(Ok(value))
            except Exception as e:
                results.append(Err(e))
        return results
