from http.cookies import BaseCookie

from osprey.engine.executor.execution_context import ExpectedUdfException

from ._prelude import ArgumentsBase, ConstExpr, ExecutionContext, UDFBase
from .categories import UdfCategories


class Arguments(ArgumentsBase):
    header: str
    """A header string to extract a cookie from."""
    key: ConstExpr[str]
    """The key of the cookie to extract from the provided header."""


class ExtractCookie(UDFBase[Arguments, str]):
    """Extracts values from a cookie."""

    category = UdfCategories.HTTP

    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> str:
        cookies: BaseCookie[str] = BaseCookie(arguments.header)
        cookie = cookies.get(arguments.key.value)
        if not cookie:
            raise ExpectedUdfException()

        return cookie.value
