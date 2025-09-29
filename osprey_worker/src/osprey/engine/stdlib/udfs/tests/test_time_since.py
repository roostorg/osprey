from datetime import datetime, timezone

import pytest
from osprey.engine.conftest import ExecuteFunction
from osprey.engine.stdlib.udfs.time_since import TimeSince
from osprey.engine.udf.registry import UDFRegistry

pytestmark = [
    pytest.mark.use_udf_registry(UDFRegistry.with_udfs(TimeSince)),
]


@pytest.mark.parametrize(
    'timestamp, seconds',
    (
        # In the past, just under 8 min 26 sec ago
        ('2020-07-16T19:51:44.533000+00:00', 495.467),
        # Same time, but as if it were from javascript (i.e. Date().toISOString())
        ('2020-07-16T19:51:44.533000621Z', 495.467),
        # In the future, so we clamp to zero
        ('2020-07-16T20:01:10.533000+00:00', 0.0),
        # Test parses Z offset correctly
        ('2020-07-16T20:01:10.533Z+00:00', 0.0),
    ),
)
def test_time_since(execute: ExecuteFunction, timestamp: str, seconds: float) -> None:
    now = datetime(2020, 7, 16, 20, tzinfo=timezone.utc)
    data = execute(f'T = TimeSince(timestamp="{timestamp}")', action_time=now)
    assert data == {'T': seconds}
