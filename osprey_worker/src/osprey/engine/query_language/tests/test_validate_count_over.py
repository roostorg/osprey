from typing import Iterator

import pytest
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.ast_validator.validators.validate_dynamic_calls_have_annotated_rvalue import (
    ValidateDynamicCallsHaveAnnotatedRValue,
)
from osprey.engine.conftest import CheckFailureFunction, RunValidationFunction
from osprey.engine.query_language.udfs.count_over import CountOver
from osprey.engine.query_language.validate_count_over import ValidateCountOver
from osprey.engine.udf.registry import UDFRegistry

pytestmark = [
    pytest.mark.use_validators(
        [
            ValidateCountOver,
            ValidateCallKwargs,
            ValidateDynamicCallsHaveAnnotatedRValue,
            UniqueStoredNames,
        ]
    ),
    pytest.mark.use_udf_registry(UDFRegistry.with_udfs(CountOver)),
]


@pytest.fixture(scope='session', autouse=True)
def postgres_database_config() -> Iterator[None]:
    """Override the root conftest's postgres fixture — validator tests don't need a DB."""
    yield


def test_golden_path_with_key(run_validation: RunValidationFunction) -> None:
    run_validation(
        "Query = CountOver(predicate=UserLoginIp == '1.1.1.1', window='10m', key=UserId) >= 10"
    )


def test_golden_path_without_key(run_validation: RunValidationFunction) -> None:
    run_validation("Query = CountOver(predicate=Endpoint == '/foo', window='1m') >= 5")


@pytest.mark.parametrize(
    'operator,threshold',
    [
        ('>=', 10),
        ('>', 10),
        ('==', 10),
        ('!=', 10),
        ('<=', 10),
        ('<', 10),
    ],
)
def test_all_six_operators_positive(run_validation: RunValidationFunction, operator: str, threshold: int) -> None:
    """Positive test: all six operators should pass validation."""
    run_validation(
        f"Query = CountOver(predicate=UserLoginIp == '1.1.1.1', window='10m') {operator} {threshold}"
    )


def test_top_level_under_not_rejected(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation(
            "Query = not (CountOver(predicate=UserLoginIp == '1.1.1.1', window='10m') >= 10)"
        )


def test_multiple_count_over_rejected(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation(
            'Query = '
            "CountOver(predicate=UserLoginIp == '1.1.1.1', window='10m') >= 10 "
            'or '
            "CountOver(predicate=Endpoint == '/foo', window='1m') >= 5"
        )


def test_nested_count_over_rejected(run_validation: RunValidationFunction, check_failure: CheckFailureFunction) -> None:
    with check_failure():
        run_validation(
            'Query = CountOver('
            "predicate=CountOver(predicate=Foo == 'x', window='1m') == 1, "
            "window='10m') >= 5"
        )


def test_rhs_must_be_integer_literal(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation(
            'SomeVar = 5\n'
            "Query = CountOver(predicate=UserLoginIp == '1.1.1.1', window='10m') >= SomeVar"
        )


def test_trivial_threshold_rejected(run_validation: RunValidationFunction, check_failure: CheckFailureFunction) -> None:
    with check_failure():
        run_validation("Query = CountOver(predicate=UserLoginIp == '1.1.1.1', window='10m') >= 0")


def test_key_must_be_column_reference(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation(
            'Query = CountOver('
            "predicate=UserLoginIp == '1.1.1.1', "
            "window='10m', "
            'key="invalid_string_literal") >= 10'
        )


def test_excessive_threshold_rejected(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation("Query = CountOver(predicate=UserLoginIp == '1.1.1.1', window='10m') >= 1000000")


def test_unsupported_comparator_rejected(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation(
            "Query = CountOver(predicate=UserLoginIp == '1.1.1.1', window='10m') in [10, 20]"
        )


def test_rule_context_rejected(run_validation: RunValidationFunction, check_failure: CheckFailureFunction) -> None:
    with check_failure():
        run_validation(
            'AbusiveAction = Rule(\n'
            '    when_all=[\n'
            "        CountOver(predicate=UserLoginIp == '1.1.1.1', window='10m') >= 10,\n"
            '    ],\n'
            "    description='Bursty login',\n"
            ')'
        )


def test_window_must_be_string_literal(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation(
            "Query = CountOver(predicate=UserLoginIp == '1.1.1.1', window=SomeVar) >= 10"
        )


def test_window_invalid_format_rejected(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation(
            "Query = CountOver(predicate=UserLoginIp == '1.1.1.1', window='abc') >= 10"
        )


def test_unsupported_predicate_in_operator_rejected(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    """Predicates using 'in' operator (not supported by SQL emitter) should be rejected."""
    with check_failure():
        run_validation("Query = CountOver(predicate=Country in ['US'], window='10m') >= 10")


def test_unsupported_predicate_function_call_rejected(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    """Predicates using function calls (not supported by SQL emitter) should be rejected."""
    with check_failure():
        run_validation(
            "Query = CountOver(predicate=SomeUdf(UserLoginIp) == 'result', window='10m') >= 10"
        )


def test_supported_predicate_with_and(run_validation: RunValidationFunction) -> None:
    """Predicates with AND should be accepted."""
    run_validation(
        "Query = CountOver(predicate=UserLoginIp == '1.1.1.1' and Endpoint == '/foo', window='10m') >= 10"
    )


def test_supported_predicate_with_or(run_validation: RunValidationFunction) -> None:
    """Predicates with OR should be accepted."""
    run_validation(
        "Query = CountOver(predicate=UserLoginIp == '1.1.1.1' or Endpoint == '/foo', window='10m') >= 10"
    )


def test_supported_predicate_with_not(run_validation: RunValidationFunction) -> None:
    """Predicates with NOT should be accepted."""
    run_validation("Query = CountOver(predicate=not (UserLoginIp == '1.1.1.1'), window='10m') >= 10")
