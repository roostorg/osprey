from abc import ABC, abstractmethod
from collections.abc import Hashable, Sequence
from datetime import datetime, timedelta
from typing import Generic, TypeVar, cast

from gevent.event import AsyncResult
from result import Err, Ok, Result

KeyT = TypeVar('KeyT', bound=Hashable)
ValueT = TypeVar('ValueT')


class ExternalService(ABC, Generic[KeyT, ValueT]):
    @abstractmethod
    def get_from_service(self, key: KeyT) -> ValueT:
        raise NotImplementedError

    # Not abstract because not all services support batching multiple keys
    def batch_get_from_service(self, keys: Sequence[KeyT]) -> Sequence[Result[ValueT, Exception]]:
        raise NotImplementedError

    def cache_ttl(self) -> timedelta | None:
        """
        Returns a time to live for items in the cache. By default, KVs are cached indefinitely.

        To have cache entries auto-expire, override this method in your external service definition.

        Note that timedeltas can accept negative values to represent the past, but only on the days field.
        You *can* use timedelta(seconds=0) to disable caching, but a negative time delta *ensures* that even
        if a time shift occurs (such as daylight savings), the cache_ttl will still be immediate.

        Therefore, to disable the read cache, it is recommended to set this to `timedelta(days=-1)`
        """
        return None


class ExternalServiceAccessor(Generic[KeyT, ValueT]):
    """Facilitates accessing an external service in a way that caches and debounces requests based on a key."""

    def __init__(self, service: ExternalService[KeyT, ValueT]):
        self._service = service
        # Key -> tuple[ AsyncResult[ValueT], Expiration datetime ]
        self._cache: dict[KeyT, tuple[AsyncResult[ValueT], datetime | None]] = {}

    def _is_past_cache_expiration(self, cache_expiration: datetime | None) -> bool:
        """
        Helper method to perform a time check on an optional datetime.
        """
        if cache_expiration is None:
            return False
        return datetime.now() > cache_expiration

    def _get_cache_expiration_datetime(self) -> datetime | None:
        """
        Helper method to generate an optional cache expiration datetime based on the cache TTL.
        """
        ttl = self._service.cache_ttl()
        return datetime.now() + ttl if ttl is not None else None

    def get_without_cache(self, key: KeyT) -> ValueT:
        """
        Ignores any cached values and performs a read-through `get` to the external service.
        The new value is then used to update the cache entry for subsequent `get` calls.
        """
        # Provide an explicit type annotation for cache_entry.
        cache_entry: tuple[AsyncResult[ValueT], datetime | None] = (
            AsyncResult(),
            self._get_cache_expiration_datetime(),
        )
        self._cache[key] = cache_entry
        try:
            cache_entry[0].set(self._service.get_from_service(key))
        except Exception as e:
            cache_entry[0].set_exception(e)

        # Cast the returned value to ValueT.
        return cast(ValueT, cache_entry[0].get())

    def get(self, key: KeyT) -> ValueT:
        # No lock needed since the check-and-update happens without IO.
        cache_entry = self._cache.get(key)
        if cache_entry is None or self._is_past_cache_expiration(cache_entry[1]):
            cache_entry = (AsyncResult(), self._get_cache_expiration_datetime())
            self._cache[key] = cache_entry
            try:
                cache_entry[0].set(self._service.get_from_service(key))
            except Exception as e:
                cache_entry[0].set_exception(e)

        return cast(ValueT, cache_entry[0].get())

    def batch_get(self, keys: Sequence[KeyT]) -> Sequence[Result[ValueT, Exception]]:
        # No lock needed since the check-and-update happens without IO.
        cached_entries = [self._cache.get(key) for key in keys]
        non_cached_keys = [
            key
            for key, cache_entry in zip(keys, cached_entries)
            if cache_entry is None or self._is_past_cache_expiration(cache_entry[1])
        ]
        if non_cached_keys:
            for key in non_cached_keys:
                self._cache[key] = (AsyncResult(), self._get_cache_expiration_datetime())
            try:
                result = self._service.batch_get_from_service(non_cached_keys)
                for i, key in enumerate(non_cached_keys):
                    if result[i].is_ok():
                        self._cache[key][0].set(result[i].value)
                    else:
                        # Cast the exception to BaseException as expected by set_exception.
                        self._cache[key][0].set_exception(cast(BaseException, result[i].value))
            except Exception as e:
                for key in non_cached_keys:
                    self._cache[key][0].set_exception(e)

        return [
            # Cast the value to ValueT, ensuring Ok receives a non-None value.
            Ok(cast(ValueT, self._cache[key][0].get()))
            if self._cache[key][0].exception is None
            else Err(cast(Exception, self._cache[key][0].exception))
            for key in keys
        ]
