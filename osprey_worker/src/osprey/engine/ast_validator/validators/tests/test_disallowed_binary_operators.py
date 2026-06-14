from typing import Any, Callable, List

import pytest
from osprey.engine.ast_validator.validators.imports_must_not_have_cycles import ImportsMustNotHaveCycles
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.ast_validator.validators.validate_dynamic_calls_have_annotated_rvalue import (
    ValidateDynamicCallsHaveAnnotatedRValue,
)
from osprey.engine.ast_validator.validators.validate_static_types import ValidateStaticTypes
from osprey.engine.ast_validator.validators.variables_must_be_defined import VariablesMustBeDefined
from osprey.engine.conftest import CheckFailureFunction, RunValidationFunction
from osprey.engine.udf.registry import UDFRegistry

pytestmark: List[Callable[[Any], Any]] = [
    pytest.mark.use_udf_registry(UDFRegistry.with_udfs()),
    pytest.mark.use_validators(
        [
            ValidateStaticTypes,
            # Dependencies of ValidateStaticTypes
            ImportsMustNotHaveCycles,
            UniqueStoredNames,
            ValidateCallKwargs,
            ValidateDynamicCallsHaveAnnotatedRValue,
            VariablesMustBeDefined,
        ]
    ),
]


def test_rejects_pow_operator(run_validation: RunValidationFunction, check_failure: CheckFailureFunction) -> None:
    with check_failure():
        run_validation('Foo = 2 ** 8')


def test_rejects_left_shift_operator(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation('Foo = 1 << 8')


def test_rejects_right_shift_operator(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation('Foo = 256 >> 2')


def test_allows_other_arithmetic_operators(run_validation: RunValidationFunction) -> None:
    # Regression guard: the restriction must reject only **, <<, and >>, not ordinary arithmetic.
    run_validation(
        """
        A = 1 + 2
        B = 3 - 1
        C = 2 * 4
        D = 8 / 2
        E = 9 // 2
        F = 9 % 2
        G = 6 | 1
        H = 6 & 2
        I = 6 ^ 1
        """
    )
