from typing import Any, Callable, Dict, List

import pytest
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.ast_validator.validators.validate_dynamic_calls_have_annotated_rvalue import (
    ValidateDynamicCallsHaveAnnotatedRValue,
)
from osprey.engine.conftest import (
    CheckFailureFunction,
    ExecuteFunction,
    ExecuteWithResultFunction,
    RunValidationFunction,
)
from osprey.engine.stdlib.udfs.json_data import JsonData
from osprey.engine.udf.registry import UDFRegistry

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


def test_execute_nested_path(execute: ExecuteFunction, execute_with_result: ExecuteWithResultFunction) -> None:
    source = "Foo: str = JsonData(path='$.a.b.c', required=False)"

    assert execute(source, data={'a': {'b': {'c': 'hello'}}}) == {'Foo': 'hello'}
    # Missing leaf, missing intermediate, and a non-dict intermediate all read as absent.
    for data in ({'a': {'b': {}}}, {'a': {}}, {'a': None}, {'a': ['b']}, {}):
        result = execute_with_result(source, data=data)
        assert not result.error_infos, (data, result.error_infos)
        assert result.extracted_features['Foo'] is None


@pytest.mark.parametrize(
    ('path', 'data'),
    [
        ('$.a.*', {'a': {'k': 'x'}}),
        ('$.a[0]', {'a': ['x']}),
        ('$..c', {'b': {'c': 'x'}}),
    ],
)
def test_execute_non_simple_path_uses_jsonpath_rw(path: str, data: Dict[str, Any], execute: ExecuteFunction) -> None:
    """Wildcard/index/descendant paths can't be specialized, so they keep the jsonpath_rw walk."""
    assert execute(f"Foo: str = JsonData(path='{path}')", data=data) == {'Foo': 'x'}


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


@pytest.mark.parametrize('bad_value', ['267000.0', '', 'not-a-number'])
def test_execute_uncoercible_optional_treated_as_absent(
    execute_with_result: ExecuteWithResultFunction, bad_value: str
) -> None:
    """A present-but-uncoercible value on a non-required field is treated like a missing one
    (no error reported), instead of raising a noisy InvalidJsonType.

    Regression for the most frequent rules-sink error (client_build_number sent as a non-int
    string such as a float-formatted, empty, or non-numeric value).
    """
    result = execute_with_result(
        "Foo: Optional[int] = JsonData(path='$.foo', coerce_type=True, required=False)",
        data={'foo': bad_value},
    )
    assert not result.error_infos, (bad_value, result.error_infos)
    assert result.extracted_features['Foo'] is None


def test_execute_uncoercible_required_still_raises(execute_with_result: ExecuteWithResultFunction) -> None:
    """A `required` field still raises InvalidJsonType for an uncoercible value — only
    non-required fields degrade to absent."""
    result = execute_with_result(
        "Foo: int = JsonData(path='$.foo', coerce_type=True, required=True)",
        data={'foo': 'not-a-number'},
    )
    assert len(result.error_infos) == 1, result.error_infos
    assert '$.foo' in str(result.error_infos[0].error)
