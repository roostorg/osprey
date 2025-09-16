from typing import Any, List, Optional

import pytest

from ....conftest import ExecuteFunction
from ....osprey_udf.registry import UDFRegistry
from ..list_length import ListLength

pytestmark = [pytest.mark.use_udf_registry(UDFRegistry.with_udfs(ListLength))]


@pytest.mark.parametrize('input,expected_length', [(['hello', 'world'], 2), ([1, 2, 3, 4], 4), ([], 0), ([None], 1)])
def test_list_length(execute: ExecuteFunction, input: Optional[List[Any]], expected_length: int) -> None:
    data = execute(
        f"""
        Result = ListLength(list={str(input)})
        """
    )
    assert data == {'Result': expected_length}
