from typing import Any, Callable, List

import pytest
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.ast_validator.validators.validate_dynamic_calls_have_annotated_rvalue import (
    ValidateDynamicCallsHaveAnnotatedRValue,
)

from ....conftest import CheckFailureFunction, ExecuteFunction, ExecuteWithResultFunction, RunValidationFunction
from ....osprey_udf.registry import UDFRegistry
from ..json_data import JsonData

pytestmark: List[Callable[[Any], Any]] = [
    pytest.mark.use_validators([ValidateCallKwargs, ValidateDynamicCallsHaveAnnotatedRValue, UniqueStoredNames]),
    pytest.mark.use_udf_registry(UDFRegistry.with_udfs(JsonData)),
]


def test_accepts_valid_json_data_path(run_validation: RunValidationFunction) -> None:
    run_validation("Foo: str = JsonData(path='$.foo.bar')")


def test_rejects_invalid_json_data_path(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation("Foo: str = JsonData(path='$..')")


def test_execute(execute: ExecuteFunction) -> None:
    data = execute("Foo: str = JsonData(path='$.foo')", data={'foo': 'hello'})

    assert data == {'Foo': 'hello'}


def test_execute_value_not_present(execute: ExecuteFunction, execute_with_result: ExecuteWithResultFunction) -> None:
    result = execute_with_result("Foo: str = JsonData(path='$.foo', required=False)", data={})
    assert not result.error_infos
    assert result.extracted_features['Foo'] is None

    result = execute_with_result("Foo: str = JsonData(path='$.foo', required=True)", data={})
    assert len(result.error_infos) == 1, result.error_infos
    error_message = str(result.error_infos[0].error)
    assert '$.foo' in error_message
    assert result.extracted_features['Foo'] is None

    result = execute_with_result("Foo: Optional[str] = JsonData(path='$.foo', required=True)", data={})
    assert not result.error_infos
    assert result.extracted_features['Foo'] is None

    result = execute_with_result("Foo: Optional[str] = JsonData(path='$.foo', required=False)", data={})
    assert not result.error_infos
    assert result.extracted_features['Foo'] is None


def test_execute_value_present_but_null(execute: ExecuteFunction) -> None:
    data = execute("Foo: str = JsonData(path='$.foo', required=False)", data={'foo': None})

    assert data == {'Foo': None}


def test_execute_coerce_type(execute: ExecuteFunction) -> None:
    data = execute(
        """
        Foo: int = JsonData(path='$.foo', coerce_type=True)
        Bar: int = JsonData(path='$.bar', coerce_type=True, required=False)
        Foo2: Optional[int] = JsonData(path='$.foo', coerce_type=True, required=False)
        """,
        data={'foo': '123', 'bar': None},
    )

    assert data == {'Foo': 123, 'Bar': None, 'Foo2': 123}
