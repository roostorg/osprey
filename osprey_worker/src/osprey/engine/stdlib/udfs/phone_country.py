from osprey.engine.executor.execution_context import ExpectedUdfException
from phone_iso3166.country import phone_country
from phone_iso3166.errors import InvalidPhone

from ._prelude import ArgumentsBase, ExecutionContext, UDFBase
from .categories import UdfCategories


class Arguments(ArgumentsBase):
    phone_number: str


class PhoneCountry(UDFBase[Arguments, str]):
    """
    Returns the 2-letter ISO-3166 country code associated with a Phone Number.

    Example usage:
      PhoneCountry(phone_number="5558675309")
    """

    category = UdfCategories.PHONE

    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> str:
        try:
            country_alpha_2 = phone_country(arguments.phone_number)
        except InvalidPhone:
            raise ExpectedUdfException()
        if not country_alpha_2:
            raise ExpectedUdfException()
        return country_alpha_2
