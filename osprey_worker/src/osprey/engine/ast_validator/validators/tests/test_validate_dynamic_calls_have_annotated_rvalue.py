import pytest
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.ast_validator.validators.validate_dynamic_calls_have_annotated_rvalue import (
    ValidateDynamicCallsHaveAnnotatedRValue,
)
from osprey.engine.conftest import CheckFailureFunction, RunValidationFunction

pytestmark = [
    pytest.mark.use_validators([ValidateCallKwargs, ValidateDynamicCallsHaveAnnotatedRValue, UniqueStoredNames]),
    pytest.mark.use_osprey_stdlib,
]


def test_missing_annotation(run_validation: RunValidationFunction, check_failure: CheckFailureFunction) -> None:
    with check_failure():
        run_validation("Foo = JsonData(path='$.foo')")


def test_missing_annotation_and_argument(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation('Foo = JsonData()')


@pytest.mark.parametrize(
    'annotation',
    (
        'wut',  # Invalid type
        'str | str',  # Duplicate arg
        'str | wut',  # Invalid arg type
        'Union',  # Missing arg
        'list[str, str]',  # Extra arg
        'list[str, int, float]',  # Extra args with different types
        'list[wut]',  # Invalid arg type
        'list',  # Missing arg
        'str | str | None',  # Extra arg
        'str | int | float | None',  # Extra args with different types
        'wut | None',  # Invalid arg type
        'Optional',  # Missing arg
    ),
)
def test_invalid_annotation(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction, annotation: str
) -> None:
    with check_failure():
        run_validation(f"Foo: {annotation} = JsonData(path='$.foo')")


def test_valid_annotation_list(run_validation: RunValidationFunction) -> None:
    run_validation("Foo: list[str] = JsonData(path='$.foo')")


def test_dynamic_fn_must_be_assigned(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation("Foo = Entity(type='User', id=JsonData(path='$.foo'))")


def test_generic_fn_must_be_assigned(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation("Foo = [EntityJson(type='User', path='$.foo')]")


def test_generic_fn_must_have_compatible_type(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation("A: str = EntityJson(type='User', path='$.foo')")
