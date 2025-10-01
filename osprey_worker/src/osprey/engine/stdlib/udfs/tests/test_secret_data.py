from typing import Any, Callable, Dict, List, Optional

import pytest
from osprey.engine.conftest import CheckFailureFunction, ExecuteFunction, RunValidationFunction
from osprey.engine.stdlib.udfs.json_data import JsonData
from osprey.engine.stdlib.udfs.string import StringClean, StringLength
from osprey.engine.udf.registry import UDFRegistry
from typing_extensions import TypedDict

pytestmark: List[Callable[[Any], Any]] = [
    pytest.mark.use_standard_rules_validators(),
    pytest.mark.use_udf_registry(UDFRegistry.with_udfs(JsonData, StringClean, StringLength)),
]

is_secret_mark = pytest.mark.parametrize(
    'is_secret,input_data',
    [
        (True, {'data': None, 'secret_data': {'foo': 'foo contents'}}),
        (False, {'data': {'foo': 'foo contents'}, 'secret_data': None}),
    ],
    ids=['with-secret', 'with-regular'],
)


class DataT(TypedDict):
    data: Optional[Dict[str, object]]
    secret_data: Optional[Dict[str, str]]


@pytest.mark.use_standard_rules_validators()
@is_secret_mark
def test_secret_read(
    is_secret: bool,
    input_data: DataT,
    execute: ExecuteFunction,
    run_validation: RunValidationFunction,
    check_failure: CheckFailureFunction,
) -> None:
    data = execute(
        """
        Foo: Secret[str] = JsonData(path='$.foo', required=False)
        FoundFoo: bool = Foo == 'foo contents'  # secret content can transition to bools
        FoundFooIsNone: bool = Foo == None
        """,
        **input_data,
    )
    if is_secret:
        assert data == {'FoundFoo': True, 'FoundFooIsNone': False}, 'foo should be extracted but not exported'
    else:
        assert data == {
            'FoundFoo': False,
            'FoundFooIsNone': True,
        }, 'foo should not be found because it is expected in the secret_data'


@pytest.mark.use_standard_rules_validators()
@is_secret_mark
def test_secret_type(
    is_secret: bool,
    input_data: DataT,
    execute: ExecuteFunction,
    run_validation: RunValidationFunction,
) -> None:
    data = execute(
        "Foo: Secret[str] = JsonData(path='$.foo', required=False)",
        **input_data,
    )
    assert data == {}, 'the data should be empty because Foo is marked as Secret'


@pytest.mark.use_standard_rules_validators()
@is_secret_mark
def test_secret_type_fails_format_string(
    is_secret: bool,
    input_data: DataT,
    execute: ExecuteFunction,
    run_validation: RunValidationFunction,
    check_failure: CheckFailureFunction,
) -> None:
    with check_failure():
        execute(
            """
            Foo: Secret[str] = JsonData(path='$.foo', required=False)
            FormattedFoo = f'{Foo}'
            """,
            **input_data,
        )


@pytest.mark.use_standard_rules_validators()
@is_secret_mark
def test_secret_type_fails_list(
    is_secret: bool,
    input_data: DataT,
    execute: ExecuteFunction,
    run_validation: RunValidationFunction,
    check_failure: CheckFailureFunction,
) -> None:
    with check_failure():
        execute(
            """
            Foo: Secret[str] = JsonData(path='$.foo', required=False)
            ListFoo = [Foo]
            """,
            **input_data,
        )


@pytest.mark.use_standard_rules_validators()
@is_secret_mark
def test_secret_type_fails_call(
    is_secret: bool,
    input_data: DataT,
    execute: ExecuteFunction,
    run_validation: RunValidationFunction,
    check_failure: CheckFailureFunction,
) -> None:
    with check_failure():
        execute(
            """
            Foo: Secret[str] = JsonData(path='$.foo', required=False)
            CallFoo = StringClean(s=Foo)
            """,
            **input_data,
        )


@pytest.mark.use_standard_rules_validators()
@is_secret_mark
def test_secret_type_passes_transform_call(
    is_secret: bool,
    input_data: DataT,
    execute: ExecuteFunction,
    run_validation: RunValidationFunction,
    check_failure: CheckFailureFunction,
) -> None:
    data = execute(
        """
        Foo: Secret[str] = JsonData(path='$.foo', required=False)
        CallLen: ExtractSecret[int] = StringLength(s=Foo)
        StrWithCallLen: str = f'len is {CallLen}'
        """,
        **input_data,
    )

    if is_secret:
        assert input_data['secret_data']
        foo = input_data['secret_data']['foo']
        assert data == {'CallLen': len(foo), 'StrWithCallLen': f'len is {len(foo)}'}
    else:
        assert data == {'CallLen': None, 'StrWithCallLen': None}


@pytest.mark.use_standard_rules_validators()
@is_secret_mark
def test_secret_data_in_operator(
    is_secret: bool,
    input_data: DataT,
    execute: ExecuteFunction,
) -> None:
    data = execute(
        """
        Foo: Secret[str] = JsonData(path='$.foo', required=False)
        FooContainsfoo: bool = 'foo' in Foo
        """,
        **input_data,
    )
    if is_secret:
        assert data == {'FooContainsfoo': True}, 'Foo should contain the string foo'
    else:
        assert data == {
            'FooContainsfoo': None,
        }, 'Foo should resolve to None and therefore not entire expression should resolve to None'
