import pytest
from osprey.engine.conftest import ExecuteFunction
from osprey.engine.stdlib.udfs.parse_int import ParseInt
from osprey.engine.udf.registry import UDFRegistry

pytestmark = [pytest.mark.use_udf_registry(UDFRegistry.with_udfs(ParseInt))]


@pytest.mark.parametrize('s,expected', [('15', 15), ('04', 4), ('0', 0), ('-7', -7), ('-092', -92)])
def test_parse_int(execute: ExecuteFunction, s: str, expected: int) -> None:
    data = execute(f'Result = ParseInt(s="{s}")')
    assert data == {'Result': expected}


def test_parse_int_invalid(execute: ExecuteFunction) -> None:
    data = execute('Result = ParseInt(s="ABC")')
    assert data == {'Result': None}
