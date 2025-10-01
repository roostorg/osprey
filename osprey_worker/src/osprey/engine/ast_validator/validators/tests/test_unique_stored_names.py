import pytest
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.conftest import CheckFailureFunction, RunValidationFunction

pytestmark = pytest.mark.use_validators([UniqueStoredNames])


def test_unique_stored_names_succeeds(run_validation: RunValidationFunction) -> None:
    ctx = run_validation(
        """
        Foo = 1
        Bar = 2
        """
    )
    assert set(ctx.get_validator_result(UniqueStoredNames).keys()) == {'Foo', 'Bar'}


def test_unique_stored_names_fails_with_dupe_names_in_same_file(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation(
            """
            Foo = 1
            Foo = 2
            Foo = 3
            """
        )


def test_unique_stored_names_fails_with_dupe_names_in_different_file(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation({'main.sml': 'Foo = 1', 'foo.sml': 'Foo = 1'})


def test_unique_stored_names_fails_allows_duplicate_locals_in_different_files(
    run_validation: RunValidationFunction,
) -> None:
    run_validation({'main.sml': '_Foo = 1', 'foo.sml': '_Foo = 1'})


def test_unique_stored_names_fails_disallows_duplicate_locals_in_same_file(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation(
            """
            _Foo = 1
            _Foo = 2
            """
        )
