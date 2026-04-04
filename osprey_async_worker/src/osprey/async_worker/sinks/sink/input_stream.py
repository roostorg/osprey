"""Async input streams for the async worker."""

import abc
from collections import deque
from typing import AsyncIterator, Generic, Sequence, TypeVar

from osprey.engine.executor.execution_context import Action
from osprey.worker.sinks.utils.acking_contexts_base import BaseAckingContext, NoopAckingContext

_T = TypeVar('_T')


class AsyncBaseInputStream(abc.ABC, Generic[_T]):
    """Async version of BaseInputStream. Uses async iteration."""

    def __aiter__(self) -> AsyncIterator[_T]:
        return self._gen()

    @abc.abstractmethod
    async def _gen(self) -> AsyncIterator[_T]:
        raise NotImplementedError
        yield  # make this an async generator

    async def stop(self) -> None:
        pass


class AsyncStaticInputStream(AsyncBaseInputStream[_T]):
    """An async input stream that returns a static list, until exhausted. For testing."""

    def __init__(self, items: Sequence[_T]):
        self._items = deque(items)

    async def _gen(self) -> AsyncIterator[_T]:
        for item in self._items:
            yield item
