from typing import Any

import pytest
from osprey.engine.conftest import CheckFailureFunction, ExecuteFunction, RunValidationFunction


def test_num_literal(execute: ExecuteFunction) -> None:
    data = execute(
        """
        One: ExtractLiteral[int] = 1
        """
    )

    assert data == {'One': 1}


def test_string_literal(execute: ExecuteFunction) -> None:
    data = execute(
        """
        Str: ExtractLiteral[str] = "hello world"
        """
    )

    assert data == {'Str': 'hello world'}


@pytest.mark.xfail(reason="these don't work yet, but would be nice to have.")
@pytest.mark.parametrize(
    'name,type,expected',
    [
        # Osprey doesn't support these yet
        ('ListOptionalInt', 'List[Optional[int]]', [1, 2, None, 3]),
        ('OptionalListIntIsNone', 'Optional[List[int]]', None),
        ('OptionalListIntIsList', 'Optional[List[int]]', [1, 2, 3]),
        ('ListList', 'List[List[int]]', [[3, 4], [5, 6, 7]]),
    ],
    ids=str,
)
def test_list_literal_xfail(
    name: str, type: str, expected: Any, execute: ExecuteFunction, run_validation: RunValidationFunction
) -> None:
    data = execute(f'{name}: ExtractLiteral[{type}] = {expected}')
    assert data[name] == expected


@pytest.mark.parametrize(
    'name,type,expected',
    [
        ('ListInt', 'List[int]', [1, 2, 3]),
        ('ListFloat', 'List[float]', [2.5, 3.4]),
        ('ListStr', 'List[str]', ['hello', 'hi', 'hey']),
        ('ListBool', 'List[bool]', [True, False]),
        ('ListNone', 'List[None]', [None, None]),
    ],
    ids=str,
)
def test_list_literal(
    name: str, type: str, expected: Any, execute: ExecuteFunction, run_validation: RunValidationFunction
) -> None:
    data = execute(f'{name}: ExtractLiteral[{type}] = {expected}')
    assert data[name] == expected


def test_bool_literal(execute: ExecuteFunction) -> None:
    data = execute(
        """
        T: ExtractLiteral[bool] = True
        F: ExtractLiteral[bool] = False
        """
    )

    assert data == {'T': True, 'F': False}


def test_none_literal(execute: ExecuteFunction) -> None:
    data = execute(
        """
        N: ExtractLiteral[None] = None
        """
    )

    assert data == {'N': None}


def test_fstring_literal(execute: ExecuteFunction) -> None:
    data = execute(
        """
        Foo: ExtractLiteral[str] = "hello"
        Bar: ExtractLiteral[str] = "world"
        Baz: ExtractLiteral[str] = f"[{Foo} - {Bar}]"
        """
    )
    assert data == {'Bar': 'world', 'Baz': '[hello - world]', 'Foo': 'hello'}


def test_export_fails_on_non_literal(check_failure: CheckFailureFunction, execute: ExecuteFunction) -> None:
    with check_failure():
        execute(
            """
            IntA: int = 1
            IntB: int = 2
            Foo: ExtractLiteral[int] = IntA + IntB
            """
        )
