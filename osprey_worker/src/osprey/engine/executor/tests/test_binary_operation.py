import pytest

from ...conftest import ExecuteFunction


@pytest.mark.parametrize(
    'expr, result',
    [
        ('1 + 2', 1 + 2),
        ('1 - 2', 1 - 2),
        ('1 * 2', 1 * 2),
        ('1 / 2.0', 1 / 2.0),
        ('1 / 2', 1 / 2),
        ('1 // 2.0', 1 // 2.0),
        ('1 % 2', 1 % 2),
        ('1 ** 2', 1**2),
        ('1 << 2', 1 << 2),
        ('1 >> 2', 1 >> 2),
        ('1 | 2', 1 | 2),
        ('1 & 2', 1 & 2),
        ('1 ^ 2', 1 ^ 2),
    ],
)
def test_binary_operation(execute: ExecuteFunction, expr: str, result: int) -> None:
    data = execute(
        f"""
        Ret = {expr}
        """
    )
    assert data['Ret'] == result
