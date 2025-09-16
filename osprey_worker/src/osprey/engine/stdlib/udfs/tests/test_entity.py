from typing import Any, Callable, List

import pytest
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs

from ....conftest import CheckFailureFunction, ExecuteFunction, ExecuteWithResultFunction, RunValidationFunction
from ....osprey_udf.registry import UDFRegistry
from ..email_domain import EmailDomain
from ..entity import Entity, EntityJson
from ..import_ import Import

pytestmark: List[Callable[[Any], Any]] = [
    pytest.mark.use_validators([ValidateCallKwargs, UniqueStoredNames]),
    pytest.mark.use_udf_registry(UDFRegistry.with_udfs(EmailDomain, Entity, EntityJson, Import)),
]


def test_entity_converts_post_execution(execute: ExecuteFunction) -> None:
    data = execute(
        """
        UserName: Entity[str] = EntityJson(type='Name', path='$.user.name')
        UserId: Entity[int] = EntityJson(type='User', path='$.user.id')
        UserEmailDomain = Entity(type='EmailDomain', id='foo.bar')
        SomeIntEntity = Entity(type='MyEntity', id=4321)
        """,
        data={'user': {'name': 'Wumpus', 'id': 1234}},
    )

    assert data == {'UserName': 'Wumpus', 'UserId': 1234, 'UserEmailDomain': 'foo.bar', 'SomeIntEntity': 4321}


def test_entity_json_requires_valid_type_and_path_literals(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation(
            """
            Type = "ExampleType1"
            Path = "$.user.name"
            EntityJson(type=Type, path=Path)
            EntityJson(type='ExampleType2', path="invalid path")
            EntityJson(type=Type, path=Path)
            EntityJson(type='ExampleType2', path="invalid path")
            """
        )


def test_entity_literal_only_one_level_deep_name_resolution(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation(
            """
            Type = "ExampleType1"
            Path = "$.user.name"
            Path2 = Path
            Type2 = Type
            EntityJson(type=Type2, path=Path2)
            """
        )


def test_entity_literal_arguments_can_be_names(
    execute_with_result: ExecuteWithResultFunction, check_failure: CheckFailureFunction
) -> None:
    result = execute_with_result(
        """
        _LocalPath = "$.my_int"
        Type = "ExampleType1"
        A: Entity[int] = EntityJson(type=Type, path=_LocalPath)
        """,
        data={'my_int': 123},
    )
    assert result.extracted_features['A'] == 123


def test_entity_literal_arguments_can_be_names_from_other_source(
    execute_with_result: ExecuteWithResultFunction,
) -> None:
    result = execute_with_result(
        {
            'main.sml': """
            Import(rules=["bar.sml", "foo.sml"])
            A: Entity[int] = EntityJson(type=Type, path=Path)
            """,
            'foo.sml': """
            Type = "ExampleType1"
            """,
            'bar.sml': """
            Import(rules=["foo.sml"])
            Path = "$.my_int"
            """,
        },
        data={'my_int': 123},
    )

    assert result.extracted_features['A'] == 123


def test_entity_checks_json_value_type(execute_with_result: ExecuteWithResultFunction) -> None:
    result = execute_with_result(
        """
        A: Entity[str] = EntityJson(type='A', path='$.my_int')
        B: Entity[int] = EntityJson(type='B', path='$.my_str')
        """,
        data={'my_int': 123, 'my_str': 'abc'},
    )
    assert len(result.error_infos) == 2
    assert result.extracted_features['A'] is None
    assert result.extracted_features['B'] is None


def test_entity_can_coerce_type(execute: ExecuteFunction) -> None:
    data = execute(
        """
        A: Entity[str] = EntityJson(type='A', path='$.my_int', coerce_type=True)
        B: Entity[int] = EntityJson(type='B', path='$.my_str', coerce_type=True)
        """,
        data={'my_int': 123, 'my_str': '456'},
    )

    assert data == {'A': '123', 'B': 456}


def test_entity_can_be_optional(execute: ExecuteFunction, execute_with_result: ExecuteWithResultFunction) -> None:
    data = execute("A: Entity[str] = EntityJson(type='A', path='$.missing', required=False)")

    assert data == {'A': None}

    data = execute("A: Entity[str] = EntityJson(type='A', path='$.is_null', required=False)", data={'is_null': None})

    assert data == {'A': None}

    result = execute_with_result("A: Entity[str] = EntityJson(type='A', path='$.missing')")

    assert len(result.error_infos) == 1
    error_message = str(result.error_infos[0].error)
    assert '$.missing' in error_message

    assert result.extracted_features['A'] is None


def test_entity_equality(execute: ExecuteFunction) -> None:
    data = execute(
        """
        EStr = Entity(type='my_str', id='hello')
        StrShouldBeTrue = EStr == 'hello'
        StrShouldBeFalse = EStr == 'world'
        EntShouldBeTrue = EStr == Entity(type='my_str', id='hello')
        EntShouldBeFalse = EStr == Entity(type='different_type', id='hello')
        """
    )

    assert data == {
        'EStr': 'hello',
        'StrShouldBeTrue': True,
        'StrShouldBeFalse': False,
        'EntShouldBeTrue': True,
        'EntShouldBeFalse': False,
    }
