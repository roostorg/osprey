from typing import Any

import pytest
from osprey.engine.conftest import ExecuteFunction
from osprey.engine.stdlib.udfs.list_read import ListRead
from osprey.engine.udf.registry import UDFRegistry

pytestmark = [pytest.mark.use_udf_registry(UDFRegistry.with_udfs(ListRead))]


@pytest.mark.parametrize(
    'input,expected_str,index',
    [
        (['118', '2', '5', '30'], '118', 0),
        (['world', 'hello'], 'world', 0),
        ([1, 2, 3, 4], '1', 0),
        ([1, 2, 3, 4], '3', 2),
        (['118', '2', '5', '30'], '2', 1),
    ],
)
def test_list_read(execute: ExecuteFunction, input: list[Any] | None, expected_str: str, index: int) -> None:
    data = execute(
        f"""
        Result = ListRead(list={input}, index={index})
        """
    )
    assert data == {'Result': expected_str}


@pytest.mark.parametrize(
    'input_fail,index_fail',
    [
        (['118', '2'], 3),
        ([], 3),
    ],
)
def test_list_read_failures(execute: ExecuteFunction, input_fail: list[Any] | None, index_fail: int) -> None:
    data = execute(
        f"""
        Result = ListRead(list={input_fail}, index={index_fail})
        """
    )
    assert data == {'Result': None}
