from typing import Any, List

from ._prelude import ArgumentsBase, ExecutionContext, UDFBase
from .categories import UdfCategories


class Arguments(ArgumentsBase):
    list: List[Any]


class ListLength(UDFBase[Arguments, int]):
    """Returns the length of a list."""

    category = UdfCategories.ENGINE

    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> int:
        return len(arguments.list)
