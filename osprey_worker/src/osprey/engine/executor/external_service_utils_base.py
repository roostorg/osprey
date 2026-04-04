"""Base external service utilities — no gevent dependency.

Contains ExternalService ABC, PlainExternalServiceAccessor, and cache helpers.
The gevent-dependent ExternalServiceAccessor remains in external_service_utils.py.
"""

from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any, Dict, Generic, Hashable, Optional, Sequence, Tuple, TypeVar, cast

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


class _CacheEntry(Generic[ValueT]):
    """Value-or-exception container. No gevent, no asyncio."""

    __slots__ = ('value', 'exception')

    def __init__(self) -> None:
        self.value: Any = None
        self.exception: Optional[BaseException] = None

    def set_value(self, value: ValueT) -> None:
        self.value = value

    def set_exception(self, exc: BaseException) -> None:
        self.exception = exc

    def get(self) -> ValueT:
        if self.exception is not None:
            raise self.exception
        return self.value


class PlainExternalServiceAccessor(Generic[KeyT, ValueT]):
    """ExternalServiceAccessor without gevent.

    Identical caching and count_error_once semantics, but uses a plain
    _CacheEntry instead of gevent.event.AsyncResult. Intended for sync
    ExternalService calls that run in a thread pool where gevent is not
    monkey-patched (e.g. the async worker's legacy batch execution path).
    """

    def __init__(self, service: ExternalService[KeyT, ValueT]):
        self._service = service
        self._cache: Dict[KeyT, Tuple[_CacheEntry[ValueT], Optional[datetime]]] = {}

    def _is_expired(self, expiration: Optional[datetime]) -> bool:
        return expiration is not None and datetime.now() > expiration

    def _expiration(self) -> Optional[datetime]:
        ttl = self._service.cache_ttl()
        return datetime.now() + ttl if ttl is not None else None

    def get(self, key: KeyT) -> ValueT:
        entry = self._cache.get(key)
        if entry is not None and not self._is_expired(entry[1]):
            return entry[0].get()

        result: _CacheEntry[ValueT] = _CacheEntry()
        self._cache[key] = (result, self._expiration())
        try:
            result.set_value(self._service.get_from_service(key))
        except Exception as e:
            if self._service.count_error_once():
                result.set_value(cast(ValueT, None))
            else:
                result.set_exception(e)
            raise
        return result.get()

    def batch_get(self, keys: Sequence[KeyT]) -> Sequence[Result[ValueT, Exception]]:
        non_cached = [
            k for k in keys
            if self._cache.get(k) is None or self._is_expired(self._cache[k][1])
        ]
        if non_cached:
            for k in non_cached:
                self._cache[k] = (_CacheEntry(), self._expiration())
            try:
                results = self._service.batch_get_from_service(non_cached)
                for i, k in enumerate(non_cached):
                    if results[i].is_ok():
                        self._cache[k][0].set_value(results[i].value)
                    else:
                        self._cache[k][0].set_exception(cast(BaseException, results[i].value))
            except Exception as e:
                for k in non_cached:
                    self._cache[k][0].set_exception(e)

        return [
            Ok(cast(ValueT, self._cache[k][0].get()))
            if self._cache[k][0].exception is None
            else Err(cast(Exception, self._cache[k][0].exception))
            for k in keys
        ]
