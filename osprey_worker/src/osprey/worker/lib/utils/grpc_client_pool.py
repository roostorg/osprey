import itertools
from collections.abc import Callable
from typing import Generic, TypeVar

T = TypeVar('T')


class GrpcClientPool(Generic[T]):
    """
    A basic gRPC client pool.

    The pool is filled by `func` during construction.
    """

    def __init__(self, size: int, func: Callable[[int], T]) -> None:
        # In HTTP/2, streams are initiated by either the client or the server.
        # The stream identifier (sequence number) must follow specific rules:
        # - For client-initiated streams, the stream ID must be an odd number.
        # - For server-initiated streams, the stream ID must be an even number.
        #
        # Since we are creating a pool of streams for clients, we start at 1 and step by 2.
        self.pool = itertools.cycle([func(i) for i in range(1, size * 2, 2)])

    def get(self) -> T:
        return next(self.pool)
