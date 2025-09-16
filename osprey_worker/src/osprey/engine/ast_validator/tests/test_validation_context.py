import pytest

from ...conftest import CheckFailureFunction, RunValidationFunction

pytestmark = [pytest.mark.use_validators([])]


def test_validation_transforms_osprey_py_ast_python_errors(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation('foo(')


def test_validation_transforms_osprey_py_ast_error(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation('Foo(1)')


def test_validation_transforms_osprey_py_ast_error_2(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation(
            """
            def foo():
                pass
            """
        )
