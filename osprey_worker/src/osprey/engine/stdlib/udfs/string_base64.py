from base64 import b64decode, b64encode

from ._prelude import ArgumentsBase, ExecutionContext, UDFBase
from .categories import UdfCategories


class Arguments(ArgumentsBase):
    input: str


class Base64Encode(UDFBase[Arguments, str]):
    """Encodes a string in Base 64."""

    category = UdfCategories.ENCODING

    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> str:
        return b64encode(arguments.input.encode()).decode()


class Base64Decode(UDFBase[Arguments, str]):
    """Decodes a string from Base 64."""

    category = UdfCategories.ENCODING

    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> str:
        return b64decode(arguments.input.encode()).decode()
