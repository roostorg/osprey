from typing import Any, Callable, List

import pytest

from ....conftest import CheckFailureFunction, RunValidationFunction
from ....osprey_udf.arguments import ArgumentsBase
from ....osprey_udf.base import UDFBase
from ....osprey_udf.registry import UDFRegistry
from ..unique_stored_names import UniqueStoredNames
from ..validate_call_rvalue import ValidateCallRValue


class EmptyArguments(ArgumentsBase):
    pass


class HasResult(UDFBase[EmptyArguments, str]):
    def execute(self, *args: object, **kwargs: object) -> str:  # type: ignore[empty-body]
        pass


class HasNoResult(UDFBase[EmptyArguments, None]):
    def execute(self, *args: object, **kwargs: object) -> None:
        pass


pytestmark: List[Callable[[Any], Any]] = [
    pytest.mark.use_validators([ValidateCallRValue, UniqueStoredNames]),
    pytest.mark.use_udf_registry(UDFRegistry.with_udfs(HasResult, HasNoResult)),
]


def test_has_no_result_and_result_unused(run_validation: RunValidationFunction) -> None:
    run_validation('HasNoResult()')


def test_has_result_and_result_unused(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation('HasResult()')


def test_has_no_result_but_argument_has_result(run_validation: RunValidationFunction) -> None:
    run_validation('HasNoResult(key=HasResult())')


def test_has_result_and_result_used(run_validation: RunValidationFunction) -> None:
    run_validation('Foo = HasResult()')


def test_not_has_result_and_result_used_as_function_argument(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation('Qux = HasNoResult()')


def test_not_has_result_and_result_used_in_list_literal(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation('Boz = Baz(qux=[1, 2, HasNoResult()])')


def test_has_result_and_result_used_in_list_literal(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    run_validation('Boz = Baz(qux=[1, 2, HasResult()])')
