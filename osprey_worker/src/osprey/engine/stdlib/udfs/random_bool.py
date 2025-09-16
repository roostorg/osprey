import random

from ._prelude import ArgumentsBase, ConstExpr, ExecutionContext, UDFBase, ValidationContext
from .categories import UdfCategories


class Arguments(ArgumentsBase):
    percentage: ConstExpr[float]


class RandomBool(UDFBase[Arguments, bool]):
    """Randomly returns `True` with `percentage` chance."""

    category = UdfCategories.RANDOM

    def __init__(self, validation_context: 'ValidationContext', arguments: Arguments):
        super().__init__(validation_context, arguments)
        if arguments.percentage.value < 0.0 or arguments.percentage.value > 1.0:
            validation_context.add_error(
                'invalid `percentage`',
                span=arguments.percentage.argument_span,
                hint='this must be between 0.0 and 1.0',
            )
        if arguments.percentage.value == 0.0:
            validation_context.add_error(
                'invalid `percentage`', span=arguments.percentage.argument_span, hint='this will always be `False`'
            )
        if arguments.percentage.value == 1.0:
            validation_context.add_error(
                'invalid `percentage`', span=arguments.percentage.argument_span, hint='this will always be `True`'
            )

    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> bool:
        return random.random() < arguments.percentage.value
