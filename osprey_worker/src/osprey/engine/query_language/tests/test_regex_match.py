from typing import Any, Callable, List

import pytest
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.ast_validator.validators.validate_dynamic_calls_have_annotated_rvalue import (
    ValidateDynamicCallsHaveAnnotatedRValue,
)
from osprey.engine.conftest import CheckFailureFunction, RunValidationFunction
from osprey.engine.query_language.udfs.regex_match import RegexMatch
from osprey.engine.udf.registry import UDFRegistry

pytestmark: List[Callable[[Any], Any]] = [
    pytest.mark.use_validators([ValidateCallKwargs, ValidateDynamicCallsHaveAnnotatedRValue, UniqueStoredNames]),
    pytest.mark.use_udf_registry(UDFRegistry.with_udfs(RegexMatch)),
]


def test_regex_match_accepts_valid_call(run_validation: RunValidationFunction) -> None:
    run_validation("RegexMatch(item=A, regex='^foo$')")


def test_regex_match_fails_with_invalid_regex(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation("RegexMatch(item=A, regex='[')")


def test_regex_match_fails_with_invalid_item_node(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation("RegexMatch(item='Jake', regex='^foo$')")
