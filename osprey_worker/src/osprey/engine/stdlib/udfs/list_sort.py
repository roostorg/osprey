from typing import Any, List

from ._prelude import ArgumentsBase, ExecutionContext, UDFBase
from .categories import UdfCategories


class Arguments(ArgumentsBase):
    list: List[Any]
    reverse: bool = False


class ListSort(UDFBase[Arguments, List[Any]]):
    """Returns a sorted list."""

    category = UdfCategories.ENGINE

    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> List[Any]:
        return sorted(arguments.list, reverse=arguments.reverse)
