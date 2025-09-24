from typing import Any, Callable, List

import pytest
from osprey.engine.stdlib.udfs.import_ import Import
from osprey.engine.udf.registry import UDFRegistry

from ....conftest import CheckFailureFunction, RunValidationFunction
from ..imports_must_not_have_cycles import ImportsMustNotHaveCycles
from ..unique_stored_names import UniqueStoredNames
from ..validate_call_kwargs import ValidateCallKwargs
from ..validate_dynamic_calls_have_annotated_rvalue import ValidateDynamicCallsHaveAnnotatedRValue

pytestmark: List[Callable[[Any], Any]] = [
    pytest.mark.use_validators(
        [ValidateCallKwargs, ImportsMustNotHaveCycles, ValidateDynamicCallsHaveAnnotatedRValue, UniqueStoredNames]
    ),
    pytest.mark.use_udf_registry(UDFRegistry.with_udfs(Import)),
]


def test_import_has_no_cycles(run_validation: RunValidationFunction) -> None:
    run_validation(
        {
            'main.sml': """
            Import(rules=["bar.sml", "foo.sml"])
            FooBar = Foo + Bar
            """,
            'foo.sml': """
            Foo = 1
            """,
            'bar.sml': """
            Bar = 2
            """,
        }
    )


def test_import_has_cycles(run_validation: RunValidationFunction, check_failure: CheckFailureFunction) -> None:
    with check_failure():
        run_validation(
            {
                'main.sml': """
                Import(rules=["bar.sml", "foo.sml"])
                FooBar = Foo + Bar
                """,
                'foo.sml': """
                Import(rules=["bar.sml"])
                Foo = 1
                """,
                'bar.sml': """
                Import(rules=["baz.sml"])
                Bar = 2
                """,
                'baz.sml': """
                Import(rules=["foo.sml"])
                Bar2 = 2
                """,
            }
        )
