from osprey.engine.executor.execution_context import ExpectedUdfException

from ._prelude import ArgumentsBase, ExecutionContext, UDFBase
from .categories import UdfCategories


class Arguments(ArgumentsBase):
    phone_number: str


class PhonePrefix(UDFBase[Arguments, str]):
    """
    Returns all but the last 4 numbers of a phone number, roughly corresponding
    to a single block of 1000 telephone numbers.

    Example usage:
      PhonePrefix(phone_number="5558675309")
    """

    category = UdfCategories.PHONE

    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> str:
        prefix = arguments.phone_number[:-4]
        if prefix == '':
            raise ExpectedUdfException()
        return prefix
