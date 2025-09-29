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
from osprey.engine.conftest import CheckFailureFunction, ExecuteFunction, RunValidationFunction
from osprey.engine.stdlib.udfs.json_data import JsonData
from osprey.engine.stdlib.udfs.resolve_optional import ResolveOptional
from osprey.engine.udf.registry import UDFRegistry

pytestmark: List[Callable[[Any], Any]] = [
    pytest.mark.use_udf_registry(UDFRegistry.with_udfs(JsonData, ResolveOptional)),
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


def test_resolve_optional_unwraps_value(execute: ExecuteFunction) -> None:
    data = execute(
        """
        Foo: Optional[str] = JsonData(path='$.foo')
        FooUnwrapped: str = ResolveOptional(optional_value=Foo)
        """,
        data={'foo': 'test'},
    )

    assert data == {'Foo': 'test', 'FooUnwrapped': 'test'}


def test_resolve_optional_errors_on_none(execute: ExecuteFunction) -> None:
    with pytest.raises(ValueError):
        execute(
            """
            Foo: Optional[str] = None
            FooUnwrapped: str = ResolveOptional(optional_value=Foo, should_report_error=True)
            """
        )

    # should_report_error=False by default so execution should not raise an exception
    execute(
        """
        Foo: Optional[str] = None
        FooUnwrapped: str = ResolveOptional(optional_value=Foo)
        """
    )


def test_resolve_optional_default_value_on_none(execute: ExecuteFunction) -> None:
    data = execute(
        """
        Foo: Optional[str] = None
        FooUnwrapped: str = ResolveOptional(optional_value=Foo, default_value='test')
        """
    )

    assert data == {'FooUnwrapped': 'test'}


def test_resolve_optional_invalid_default_value_type(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation(
            """
            Foo: Optional[str] = None
            FooUnwrapped: str = ResolveOptional(optional_value=Foo, default_value=1)
            Bar: Optional[int] = 1
            BarUnwrapper: int = ResolveOptional(optional_value=Bar, default_value='test')
            """
        )


def test_resolve_optional_invalid_optional_value_type(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation(
            """
            Foo: Optional[int] = 1
            FooUnwrapped: str = ResolveOptional(optional_value=Foo)
            """
        )
