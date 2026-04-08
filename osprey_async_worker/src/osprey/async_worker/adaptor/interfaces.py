"""Async plugin interfaces for the osprey async worker.

AsyncUDFBase extends UDFBase so it works with the existing engine machinery
(UDFRegistry, CallExecutor, validation, type checking, argument resolution).

All I/O UDFs in the async worker MUST be AsyncUDFBase subclasses — existing
sync UDFs that use gevent primitives will not work without monkey patching.
Pure-computation UDFs (no I/O) can remain as regular UDFBase and run inline.
"""

import abc
from typing import Any, ClassVar, Generic, Sequence, Tuple, TypeVar

from osprey.engine.executor.execution_context import ExecutionContext, ExecutionResult
from osprey.engine.udf.base import (
    Arguments,
    BatchableArguments,
    BatchableUDFBase,
    RValue,
    UDFBase,
)
from result import Result

_T = TypeVar('_T')


class AsyncUDFBase(UDFBase[Arguments, RValue]):
    """Native async UDF base class.

    Extends UDFBase so it integrates with UDFRegistry, CallExecutor, and
    the full validation/type-checking pipeline. The async executor detects
    AsyncUDFBase instances via isinstance() and awaits async_execute()
    directly on the event loop — no thread pool.

    The sync execute() raises so it can't accidentally be called in the
    async executor's sync path.
    """

    execute_async: ClassVar[bool] = True
    is_native_async: ClassVar[bool] = True

    def __init__(self, validation_context, arguments):
        super().__init__(validation_context, arguments)

    @classmethod
    def _get_udf_base_args(cls):
        """Override to include AsyncUDFBase in the generic origin check."""
        import typing_inspect

        for base in cls.__mro__:
            for generic_base in typing_inspect.get_generic_bases(base):
                origin = typing_inspect.get_origin(generic_base)
                if origin in (UDFBase, AsyncUDFBase) or (
                    hasattr(origin, '__mro__') and UDFBase in origin.__mro__
                ):
                    args = typing_inspect.get_args(generic_base)
                    # Only return if args are concrete (not TypeVars)
                    if args and not any(isinstance(a, TypeVar) for a in args):
                        return args

        # Fallback to parent
        return super()._get_udf_base_args()

    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> RValue:
        raise RuntimeError(
            f'{self.__class__.__name__} is a native async UDF. '
            f'Use async_execute() instead of execute().'
        )

    @abc.abstractmethod
    async def async_execute(self, execution_context: ExecutionContext, arguments: Arguments) -> RValue:
        """Override this to implement the UDF's async execution logic."""
        raise NotImplementedError


class AsyncBatchableUDFBase(BatchableUDFBase[Arguments, RValue, BatchableArguments]):
    """Native async batchable UDF base class.

    Same as AsyncUDFBase but for batchable UDFs. The async executor detects
    these and awaits async_execute_batch() directly.
    """

    is_native_async: ClassVar[bool] = True

    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> RValue:
        raise RuntimeError(
            f'{self.__class__.__name__} is a native async UDF. '
            f'Use async_execute() instead of execute().'
        )

    def execute_batch(
        self,
        execution_context: ExecutionContext,
        udfs: Sequence[UDFBase[Any, Any]],
        arguments: Sequence[BatchableArguments],
    ) -> Sequence[Result[RValue, Exception]]:
        raise RuntimeError(
            f'{self.__class__.__name__} is a native async UDF. '
            f'Use async_execute_batch() instead of execute_batch().'
        )

    @abc.abstractmethod
    async def async_execute(self, execution_context: ExecutionContext, arguments: Arguments) -> RValue:
        raise NotImplementedError

    @abc.abstractmethod
    async def async_execute_batch(
        self,
        execution_context: ExecutionContext,
        udfs: Sequence[UDFBase[Any, Any]],
        arguments: Sequence[BatchableArguments],
    ) -> Sequence[Result[RValue, Exception]]:
        raise NotImplementedError


# --- Output sinks and input streams (unchanged) ---


class AsyncBaseOutputSink(abc.ABC):
    """Async output sink."""

    timeout: float = 2.0
    max_retries: int = 0

    @abc.abstractmethod
    def will_do_work(self, result: ExecutionResult) -> bool:
        raise NotImplementedError

    @abc.abstractmethod
    async def push(self, result: ExecutionResult) -> None:
        raise NotImplementedError

    async def stop(self) -> None:
        pass
