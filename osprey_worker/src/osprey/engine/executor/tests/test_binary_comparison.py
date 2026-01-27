import pytest
from osprey.engine.conftest import ExecuteFunction


@pytest.mark.parametrize(
    'statement, expected',
    [
        ('Foo == "hello"', True),
        ('"hello" == Foo', True),
        ('"world" == Foo', False),
        ('Foo == "world"', False),
        ('Foo == Bar', False),
        ('Bar == Foo', False),
        ('Foo == Foo', True),
        ('None == None', True),
        ('Bizz == None', True),
        ('Buzz == None', False),
        ('None == Bizz', True),
        ('None == Buzz', False),
    ],
)
def test_equality(execute: ExecuteFunction, statement: str, expected: bool) -> None:
    data = execute(
        f"""
        Foo: ExtractLiteral[str] = "hello"
        Bar: ExtractLiteral[str] = "world"
        Bizz: Optional[str] = None
        Buzz: Optional[str] = "some_value"
        Ret: bool = {statement}
        """
    )

    assert data == {'Foo': 'hello', 'Bar': 'world', 'Ret': expected}


def test_in(execute: ExecuteFunction) -> None:
    # Test Item in List
    data = execute(
        """
        A = [1, 2, 3]

        T = 3 in A
        F = 6 in A
        """
    )

    assert data == {'T': True, 'F': False}

    # Test String in String
    data = execute(
        """
        A = "123"

        T1 = "23" in A
        T2 = "1" in A
        F1 = "4" in A
        F2 = "1234" in A
        """
    )

    assert data == {
        'T1': True,
        'T2': True,
        'F1': False,
        'F2': False,
    }


def test_not_in(execute: ExecuteFunction) -> None:
    # Test Item not in List
    data = execute(
        """
        A = [1, 2, 3]

        T = 4 not in A
        F = 2 not in A
        """
    )

    assert data == {
        'T': True,
        'F': False,
    }

    # Test String not in String
    data = execute(
        """
        A = "123"

        T1 = "13" not in A
        T2 = "4" not in A
        F1 = "2" not in A
        F2 = "123" not in A
        """
    )

    assert data == {
        'T1': True,
        'T2': True,
        'F1': False,
        'F2': False,
    }


@pytest.mark.parametrize(
    'statement, expected',
    [
        ('A < B', True),
        ('A <= A', True),
        ('B > A', True),
        ('B >= A', True),
        ('B < A', False),
        ('B <= A', False),
        ('A > B', False),
        ('A >= B', False),
    ],
)
def test_cmp(execute: ExecuteFunction, statement: str, expected: bool) -> None:
    data = execute(
        f"""
        A = 1
        B = 2
        Ret = {statement}
        """
    )

    assert data == {'Ret': expected}


@pytest.mark.parametrize(
    'statement, expected',
    [
        ('True or False', True),
        ('True or True', True),
        ('False or True', True),
        ('False or False or True', True),
        ('True or False or False', True),
        ('False or True or False', True),
        ('False or False', False),
    ],
)
def test_or(execute: ExecuteFunction, statement: str, expected: bool) -> None:
    data = execute(
        f"""
        Ret = {statement}
        """
    )

    assert data == {'Ret': expected}


@pytest.mark.parametrize(
    'statement, expected',
    [
        ('True and True', True),
        ('True and True and True', True),
        ('False and True and True', False),
        ('True and False and False', False),
        ('True and True and False', False),
    ],
)
def test_and(execute: ExecuteFunction, statement: str, expected: bool) -> None:
    data = execute(
        f"""
        Ret = {statement}
        """
    )

    assert data == {'Ret': expected}


@pytest.mark.parametrize(
    'opt_val, expected',
    [
        (90, True),
        (80, False),
        ('None', False),  # None value - condition short-circuits
    ],
)
def test_optional_null_check_before_comparison(
    execute: ExecuteFunction, opt_val: object, expected: bool
) -> None:
    """Test that type narrowing works for X != None and X >= 90 pattern."""
    data = execute(
        f"""
        OptVal: Optional[int] = {opt_val}
        # Type narrowing: after OptVal != None, OptVal is narrowed from Optional[int] to int
        Ret = OptVal != None and OptVal >= 90
        """
    )

    assert data == {'Ret': expected}


def test_optional_null_check_chained_narrowing(execute: ExecuteFunction) -> None:
    """Test chained type narrowing with multiple optional values."""
    data = execute(
        """
        A: Optional[int] = 100
        B: Optional[int] = 50
        # Both A and B get narrowed after their respective null checks
        Ret = A != None and B != None and A >= 90 and B >= 40
        """
    )

    assert data == {'Ret': True}


@pytest.mark.parametrize(
    'opt_val, expected',
    [
        (90, True),
        (80, False),
        ('None', True),  # None value - first condition is True, so result is True
    ],
)
def test_optional_or_pattern_null_check(
    execute: ExecuteFunction, opt_val: object, expected: bool
) -> None:
    """Test that type narrowing works for X == None or X >= 90 pattern."""
    data = execute(
        f"""
        OptVal: Optional[int] = {opt_val}
        # Type narrowing: after OptVal == None is false, OptVal is narrowed from Optional[int] to int
        Ret = OptVal == None or OptVal >= 90
        """
    )

    assert data == {'Ret': expected}
