"""Async external service utilities for the async worker.

Port of osprey.engine.executor.external_service_utils with asyncio instead of gevent.
Uses asyncio.Future instead of gevent.event.AsyncResult for cache entries.
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

    def _make_future(self) -> asyncio.Future[ValueT]:
        """Create a new Future on the running event loop."""
        return asyncio.get_running_loop().create_future()

    async def get_without_cache(self, key: KeyT) -> ValueT:
        """
        Ignores any cached values and performs a read-through `get` to the external service.
        The new value is then used to update the cache entry for subsequent `get` calls.
        """
        future: asyncio.Future[ValueT] = self._make_future()
        cache_entry: Tuple[asyncio.Future[ValueT], Optional[datetime]] = (
            future,
            self._get_cache_expiration_datetime(),
        )
        self._cache[key] = cache_entry
        try:
            result = await self._service.get_from_service(key)
            future.set_result(result)
        except Exception as e:
            future.set_exception(e)

        return await future

    async def get(self, key: KeyT) -> ValueT:
        cache_entry = self._cache.get(key)
        if cache_entry is not None and not self._is_past_cache_expiration(cache_entry[1]):
            # Cache hit — await the existing future (may still be in-flight from another caller)
            return await cache_entry[0]

        future: asyncio.Future[ValueT] = self._make_future()
        cache_entry = (future, self._get_cache_expiration_datetime())
        self._cache[key] = cache_entry
        try:
            result = await self._service.get_from_service(key)
            future.set_result(result)
        except Exception as e:
            if self._service.count_error_once():
                future.set_result(cast(ValueT, None))
            else:
                future.set_exception(e)
            raise

        return await future

    async def batch_get(self, keys: Sequence[KeyT]) -> Sequence[Result[ValueT, Exception]]:
        cached_entries = [self._cache.get(key) for key in keys]
        non_cached_keys = [
            key
            for key, cache_entry in zip(keys, cached_entries)
            if cache_entry is None or self._is_past_cache_expiration(cache_entry[1])
        ]
        if non_cached_keys:
            for key in non_cached_keys:
                self._cache[key] = (self._make_future(), self._get_cache_expiration_datetime())
            try:
                result = await self._service.batch_get_from_service(non_cached_keys)
                for i, key in enumerate(non_cached_keys):
                    if result[i].is_ok():
                        self._cache[key][0].set_result(result[i].value)
                    else:
                        self._cache[key][0].set_exception(cast(BaseException, result[i].value))
            except Exception as e:
                for key in non_cached_keys:
                    self._cache[key][0].set_exception(e)

        results: list[Result[ValueT, Exception]] = []
        for key in keys:
            future = self._cache[key][0]
            try:
                value = await future
                results.append(Ok(value))
            except Exception as e:
                results.append(Err(e))
        return results
