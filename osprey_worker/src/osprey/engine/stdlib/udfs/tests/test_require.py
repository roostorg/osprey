from typing import Any, Callable, Optional

import pytest
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.ast_validator.validators.validate_dynamic_calls_have_annotated_rvalue import (
    ValidateDynamicCallsHaveAnnotatedRValue,
)
from osprey.engine.conftest import CheckFailureFunction, ExecuteFunction, RunValidationFunction
from osprey.engine.stdlib.udfs.json_data import JsonData
from osprey.engine.stdlib.udfs.require import Require
from osprey.engine.udf.registry import UDFRegistry

pytestmark: list[Callable[[Any], Any]] = [
    pytest.mark.use_validators([ValidateCallKwargs, ValidateDynamicCallsHaveAnnotatedRValue, UniqueStoredNames]),
    pytest.mark.use_udf_registry(UDFRegistry.with_udfs(JsonData, Require)),
]

sources = {
    'main.sml': """
        ActionName: str = JsonData(path='$.action_name')
        Require(rule=f'actions/{ActionName}.sml')
    """,
    'actions/foo.sml': """
        Foo: ExtractLiteral[str] = "hello foo"
    """,
    'actions/bar.sml': """
        Bar: ExtractLiteral[str] = "hello bar"
        Baz: str = JsonData(path='$.baz')
        Require(rule=f'baz/{Baz}.sml')
    """,
    'baz/qux.sml': """
        BazQux: ExtractLiteral[str] = "hello baz qux"
    """,
}


@pytest.mark.parametrize(
    'data,expected_result',
    [
        ({'action_name': 'hello'}, {'ActionName': 'hello'}),
        ({'action_name': 'foo'}, {'ActionName': 'foo', 'Foo': 'hello foo'}),
        ({'action_name': 'bar'}, {'ActionName': 'bar', 'Bar': 'hello bar', 'Baz': None}),
        (
            {'action_name': 'bar', 'baz': 'qux'},
            {'ActionName': 'bar', 'Bar': 'hello bar', 'Baz': 'qux', 'BazQux': 'hello baz qux'},
        ),
    ],
)
def test_require(execute: ExecuteFunction, data: dict[str, object], expected_result: dict[str, Optional[str]]) -> None:
    result = execute(sources, data=data, allow_errors=True)

    assert result == expected_result


sources_require_if = {
    'main.sml': """
        ActionName: str = JsonData(path='$.action_name')
        RequireEnabled: bool = JsonData(path='$.require_enabled')
        Require(rule=f'actions/{ActionName}.sml', require_if=RequireEnabled)
    """,
    'actions/foo.sml': """
        Foo: ExtractLiteral[str] = "hello foo"
    """,
}


@pytest.mark.parametrize(
    'data,expected_result',
    [
        ({'action_name': 'foo'}, {'ActionName': 'foo', 'RequireEnabled': None}),
        ({'action_name': 'bar'}, {'ActionName': 'bar', 'RequireEnabled': None}),
        ({'action_name': 'foo', 'require_enabled': False}, {'ActionName': 'foo', 'RequireEnabled': False}),
        (
            {'action_name': 'foo', 'require_enabled': True},
            {'ActionName': 'foo', 'RequireEnabled': True, 'Foo': 'hello foo'},
        ),
    ],
)
def test_require_if(execute: ExecuteFunction, data: dict[str, object], expected_result: dict[str, object]) -> None:
    result = execute(sources_require_if, data=data, allow_errors=True)

    assert result == expected_result


def test_validates_for_nonexistent_rule_fstring(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation(
            """
            ActionName: str = JsonData(path='$.action_name')
            Require(rule=f'actions/{ActionName}.sml')
            """
        )


def test_validates_for_nonexistent_rule_string(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation(
            """
            Require(rule='actions/foo.sml')
            """
        )


def test_validates_for_invalid_rule_ast_node(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation(
            """
            Foo = "hello"
            Require(rule=Foo)
            """
        )
