import pytest
from osprey.engine.conftest import ExecuteFunction
from osprey.engine.stdlib.udfs.time_delta import TimeDelta
from osprey.engine.udf.registry import UDFRegistry

pytestmark = [
    pytest.mark.use_udf_registry(UDFRegistry.with_udfs(TimeDelta)),
]


def test_time_delta_post_execution_type(execute: ExecuteFunction) -> None:
    data = execute(
        """
        T = TimeDelta(weeks=1, days=2, hours=3, minutes=4, seconds=5)
        CompareTyped = TimeDelta(minutes=5) > TimeDelta(seconds=30)
        CompareInt = TimeDelta(minutes=5) < 30
        """
    )
    assert data == {
        'T': ((((7 + 2) * 24 + 3) * 60) + 4) * 60 + 5,  # 788,645, if you're curious
        'CompareTyped': True,
        'CompareInt': False,
    }
