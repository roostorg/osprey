import re
from typing import List, Pattern

from osprey.engine.stdlib.udfs._prelude import (
    ArgumentsBase,
    ConstExpr,
    ExecutionContext,
    UDFBase,
    ValidationContext,
)
from osprey.engine.stdlib.udfs.categories import UdfCategories


class CountRegexMatchesArguments(ArgumentsBase):
    patterns: ConstExpr[List[str]]
    """List of regex patterns to evaluate. The UDF returns the count of distinct
    patterns that find at least one match in the target."""

    target: str
    """The string to evaluate the patterns against."""

    case_insensitive: ConstExpr[bool] = ConstExpr.for_default('case_insensitive', False)
    """If `True`, all patterns are matched case-insensitively. Default `False`."""


class CountRegexMatches(UDFBase[CountRegexMatchesArguments, int]):
    """Returns the number of distinct regex patterns that match the target string.

    Each pattern in `patterns` is tested independently; the result is the count of
    patterns with at least one match. Useful for "N-of-M" rule semantics where a rule
    should fire only when several distinct keywords/categories appear in the same text.
    """

    category = UdfCategories.STRING

    def __init__(self, validation_context: 'ValidationContext', arguments: CountRegexMatchesArguments):
        super().__init__(validation_context, arguments)

        flags = re.IGNORECASE if arguments.case_insensitive.value else 0

        self._compiled: List[Pattern[str]] = []
        for pattern in arguments.patterns.value:
            try:
                self._compiled.append(re.compile(pattern, flags))
            except re.error as exc:
                validation_context.add_error(
                    message=f'invalid regex pattern {pattern!r}: {exc}',
                    span=arguments.patterns.argument_span,
                )

    def execute(self, execution_context: ExecutionContext, arguments: CountRegexMatchesArguments) -> int:
        return sum(1 for compiled in self._compiled if compiled.search(arguments.target) is not None)
