import random

from ._prelude import ArgumentsBase, ConstExpr, ExecutionContext, UDFBase, ValidationContext
from .categories import UdfCategories


class Arguments(ArgumentsBase):
    start: ConstExpr[int]
    end: ConstExpr[int]


class RandomInt(UDFBase[Arguments, int]):
    """Returns an integer between the `start` and `end` values"""

    category = UdfCategories.RANDOM

    def __init__(self, validation_context: 'ValidationContext', arguments: Arguments):
        super().__init__(validation_context, arguments)
        if arguments.start.value >= arguments.end.value:
            validation_context.add_error(
                'invalid `start`',
                span=arguments.start.argument_span,
                hint='the `start` value must be less than the `end` value',
            )

    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> int:
        return random.randint(arguments.start.value, arguments.end.value)
