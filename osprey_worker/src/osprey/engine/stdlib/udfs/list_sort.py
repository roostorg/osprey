from typing import Any

from ._prelude import ArgumentsBase, ExecutionContext, UDFBase
from .categories import UdfCategories


class Arguments(ArgumentsBase):
    list: list[Any]
    reverse: bool = False


class ListSort(UDFBase[Arguments, list[Any]]):
    """Returns a sorted list."""

    category = UdfCategories.ENGINE

    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> list[Any]:
        return sorted(arguments.list, reverse=arguments.reverse)
