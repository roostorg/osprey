from typing import Any, Callable, List

import pytest
from osprey.engine.ast_validator.validators.imports_must_not_have_cycles import ImportsMustNotHaveCycles
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.ast_validator.validators.validate_dynamic_calls_have_annotated_rvalue import (
    ValidateDynamicCallsHaveAnnotatedRValue,
)
from osprey.engine.udf.registry import UDFRegistry

from ....conftest import CheckFailureFunction, ExecuteFunction, RunValidationFunction
from ..import_ import Import

pytestmark: List[Callable[[Any], Any]] = [
    pytest.mark.use_validators(
        [ValidateCallKwargs, ImportsMustNotHaveCycles, ValidateDynamicCallsHaveAnnotatedRValue, UniqueStoredNames]
    ),
    pytest.mark.use_udf_registry(UDFRegistry.with_udfs(Import)),
]


def test_import_list_of_existing_file_succeeds(run_validation: RunValidationFunction) -> None:
    run_validation({'main.sml': "Import(rules=['foo.sml'])", 'foo.sml': 'Foo = 1'})


def test_import_of_nonexistent_file_in_list_fails(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation("Import(rules=['foo.sml'])")


def test_import_of_nonexistent_file_fails(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation("Import(rules='foo.sml')")


def test_import_of_nonexistent_file_in_list_of_many_fails(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation({'main.sml': "Import(rules=['bar.sml', 'foo.sml'])", 'foo.sml': 'Foo = 1'})


def test_import_invalid_type_fails(run_validation: RunValidationFunction, check_failure: CheckFailureFunction) -> None:
    with check_failure():
        run_validation('Import(rules=11)')


def test_import_invalid_list_item_type_fails(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation('Import(rules=[11, 12, 13])')


def test_import(execute: ExecuteFunction) -> None:
    result = execute(
        {
            'main.sml': """
                Import(rules=["bar.sml", "foo.sml"])
                FooBar = Foo + Bar
                """,
            'foo.sml': """
                Foo = 1  # don't extract, it's a literal
                Foo2 = Foo + 1
                """,
            'bar.sml': """
                Import(rules=["foo.sml"])
                Bar = Foo + 1
                """,
        }
    )

    assert result == {'Foo2': 2, 'Bar': 2, 'FooBar': 3}


def test_import_dupe_items_fails(run_validation: RunValidationFunction, check_failure: CheckFailureFunction) -> None:
    with check_failure():
        run_validation({'a.sml': 'Bar = 1', 'main.sml': "Import(rules=['a.sml', 'a.sml', 'a.sml'])"})


def test_import_unsorted_items_fails(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation(
            {
                'a.sml': 'A = 1',
                'b.sml': 'B = 2',
                'c.sml': 'C = 3',
                'main.sml': "Import(rules=['c.sml', 'b.sml', 'a.sml'])",
            }
        )
