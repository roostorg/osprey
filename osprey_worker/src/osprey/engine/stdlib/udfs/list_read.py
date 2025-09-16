from typing import Any, List

from osprey.engine.executor.execution_context import ExpectedUdfException

from ._prelude import ArgumentsBase, ExecutionContext, UDFBase
from .categories import UdfCategories


class Arguments(ArgumentsBase):
    list: List[Any] = []
    index: int = 0


class ListRead(UDFBase[Arguments, str]):
    """Returns a particular value in a list."""

    category = UdfCategories.ENGINE

    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> str:
        if arguments.list is None:
            raise ExpectedUdfException()
        try:
            return str(arguments.list[arguments.index])
        except (TypeError, IndexError):
            raise ExpectedUdfException()
