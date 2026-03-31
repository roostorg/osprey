from typing import Any

import pytest
from osprey.engine.conftest import ExecuteFunction
from osprey.engine.stdlib.udfs.list_sort import ListSort
from osprey.engine.udf.registry import UDFRegistry

pytestmark = [pytest.mark.use_udf_registry(UDFRegistry.with_udfs(ListSort))]


@pytest.mark.parametrize(
    'input,expected_sorted_list',
    [(['world', 'hello'], ['hello', 'world']), ([1, 3, 4, 2], [1, 2, 3, 4]), ([], []), ([None], [None])],
)
def test_list_length(execute: ExecuteFunction, input: list[Any] | None, expected_sorted_list: list[Any]) -> None:
    data = execute(
        f"""
        Result = ListSort(list={input})
        """
    )
    assert data == {'Result': expected_sorted_list}
