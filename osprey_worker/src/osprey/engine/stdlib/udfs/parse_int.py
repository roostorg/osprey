from osprey.engine.executor.execution_context import ExpectedUdfException

from ._prelude import ArgumentsBase, ExecutionContext, UDFBase
from .categories import UdfCategories


class Arguments(ArgumentsBase):
    s: str


class ParseInt(UDFBase[Arguments, int]):
    """Converts a numeric string to an integer."""

    category = UdfCategories.CAST

    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> int:
        try:
            return int(arguments.s)
        except ValueError:
            raise ExpectedUdfException()
