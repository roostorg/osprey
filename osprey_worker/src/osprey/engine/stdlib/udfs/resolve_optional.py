from typing import Optional, TypeVar

from osprey.engine.executor.execution_context import ExpectedUdfException
from osprey.engine.language_types.osprey_invariant_generic import OspreyInvariantGeneric

from ._prelude import ArgumentsBase, ConstExpr, ExecutionContext, UDFBase

_T = TypeVar('_T')


class Arguments(OspreyInvariantGeneric[_T], ArgumentsBase):
    optional_value: Optional[_T]
    """
    The Optional value to unwrap.
    """

    default_value: Optional[_T] = None
    """
    The default value to return if the Optional value is missing.
    `should_report_error` is ignored if there is a `default_value` present.
    """

    should_report_error: ConstExpr[bool] = ConstExpr.for_default('should_report_error', False)
    """
    Whether to report an error (e.g. to Sentry) in the event that the value is missing.
    This is ignored if there is a `default_value` present.
    """


class ResolveOptional(OspreyInvariantGeneric[_T], UDFBase[Arguments[_T], _T]):
    """Transforms from an Optional[T] into the inner T value."""

    def execute(self, execution_context: ExecutionContext, arguments: Arguments[_T]) -> _T:
        if arguments.optional_value is None:
            if arguments.default_value is not None:
                return arguments.default_value
            elif arguments.should_report_error.value:
                raise ValueError('optional_value kwarg is None')
            else:
                raise ExpectedUdfException('optional_value kwarg is None')

        return arguments.optional_value
