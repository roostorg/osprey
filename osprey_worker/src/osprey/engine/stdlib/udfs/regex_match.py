import re
from typing import List

from ._prelude import ArgumentsBase, ConstExpr, ExecutionContext, UDFBase, ValidationContext
from .categories import UdfCategories


class RegexArgumentsBase(ArgumentsBase):
    pattern: ConstExpr[str]
    """The regex pattern to evaluate."""

    case_insensitive: ConstExpr[bool] = ConstExpr.for_default('case_insensitive', False)
    """Optional: If `True`, ignores case. Default is `False`."""


class RegexUDFBase:
    def __init__(self, validation_context: 'ValidationContext', arguments: RegexArgumentsBase):
        super().__init__(validation_context, arguments)  # type: ignore

        flags = 0

        if arguments.case_insensitive.value:
            flags |= re.IGNORECASE

        with arguments.pattern.attribute_errors(message='invalid regex pattern'):
            self._compiled = re.compile(arguments.pattern.value, flags)


class RegexMatchArguments(RegexArgumentsBase):
    target: str
    """A target string to evaluate the regex pattern on."""


class RegexMatch(RegexUDFBase, UDFBase[RegexMatchArguments, bool]):
    """Returns `True` if a match for the provided regex is found."""

    category = UdfCategories.STRING

    def execute(self, execution_context: ExecutionContext, arguments: RegexMatchArguments) -> bool:
        return self._compiled.search(arguments.target) is not None


class RegexMatchMapArguments(RegexArgumentsBase):
    target: List[str]
    """A target string to evaluate the regex pattern on."""

    mode: ConstExpr[str] = ConstExpr.for_default('mode', 'any')
    """Are `all` or `any` matches required?"""


class RegexMatchMap(RegexUDFBase, UDFBase[RegexMatchMapArguments, bool]):
    """Returns `True` if a match for the provided regex is found."""

    def __init__(self, validation_context: 'ValidationContext', arguments: RegexMatchMapArguments):
        super().__init__(validation_context, arguments)

        mode = arguments.mode.value
        if mode not in ('all', 'any'):
            validation_context.add_error(
                message=f'mode must be one of `all` or `any`, not `{mode}`',
                span=arguments.mode.argument_span,
            )

        self._op = all if mode == 'all' else any

    def execute(self, execution_context: ExecutionContext, arguments: RegexMatchMapArguments) -> bool:
        print(self._op, list(self._compiled.search(target) is not None for target in arguments.target))
        return self._op(self._compiled.search(target) is not None for target in arguments.target)
