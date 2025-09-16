import pytest

from ....conftest import CheckFailureFunction, RunValidationFunction
from ..no_unused_locals import NoUnusedLocals

pytestmark = pytest.mark.use_validators([NoUnusedLocals])


def test_no_unused_locals_succeeds_if_locals_are_used(run_validation: RunValidationFunction) -> None:
    run_validation(
        """
        _Foo = 1
        _Bar = 2
        Baz = _Foo + _Bar
        """
    )


def test_ignores_undefined_locals(run_validation: RunValidationFunction) -> None:
    # The locals are undefined, but this validator can tolerate that condition, as that's checked by
    # another validator.
    run_validation(
        """
        Baz = _Foo + _Bar
        """
    )


def test_multiple_defined_locals(run_validation: RunValidationFunction) -> None:
    # The locals are defined multiple times, but this validator can tolerate that condition, as that's
    # checked by another validator.
    run_validation(
        """
        _Foo = 1
        _Foo = 2
        Foo = _Foo
        """
    )


def test_multiple_defined_interspersed_store_and_load(run_validation: RunValidationFunction) -> None:
    # The locals are defined multiple times, but this validator can tolerate that condition, as that's
    # checked by another validator.

    # The second declaration of `_Foo` is ignored, so this should cause no error.
    run_validation(
        """
        _Foo = 1
        Foo = _Foo
        _Foo = 2
        """
    )


def test_no_unused_locals(run_validation: RunValidationFunction, check_failure: CheckFailureFunction) -> None:
    with check_failure():
        run_validation(
            """
            _Foo = 1
            _Bar = 2
            _Baz = 3
            Baz = _Baz
            """
        )
