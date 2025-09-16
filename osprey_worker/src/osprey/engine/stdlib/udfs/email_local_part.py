from osprey.engine.executor.execution_context import ExpectedUdfException

from ._prelude import ArgumentsBase, ExecutionContext, UDFBase
from .categories import UdfCategories


class Arguments(ArgumentsBase):
    email: str


class EmailLocalPart(UDFBase[Arguments, str]):
    """Returns the local part of an email address (everything to the left of the @)."""

    category = UdfCategories.EMAIL

    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> str:
        try:
            local, _domain = arguments.email.rsplit('@', 1)
            return local
        except ValueError:
            raise ExpectedUdfException()
