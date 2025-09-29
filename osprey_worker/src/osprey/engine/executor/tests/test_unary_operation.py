import pytest
from osprey.engine.conftest import ExecuteFunction


@pytest.mark.parametrize(
    'expr, result',
    [
        ('not True', False),
        ('not False', True),
        ('not 1 == 2', True),
        ('not not True', True),
        ('not not not True', False),
        ('-2', -2),
        ('-2.1', -2.1),
        ('-(2)', -2),
        ('-(2 + 3)', -5),
        ('-(2 - 3)', 1),
        ('-1 + 1', 0),
        ('-(-1 + -2)', 3),
        ('-(-1 + -2.1)', 3.1),
    ],
)
def test_unary_operation(execute: ExecuteFunction, expr: str, result: bool) -> None:
    data = execute(f'Ret = {expr}')
    assert data['Ret'] == result
