from typing import Any, Callable, List

import pytest
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.conftest import CheckFailureFunction, ExecuteFunction, RunValidationFunction
from osprey.engine.stdlib.udfs.time_bucket import GetSnowflakeBucket, GetTimedeltaBucket, GetTimestampBucket
from osprey.engine.stdlib.udfs.time_delta import TimeDelta
from osprey.engine.udf.registry import UDFRegistry
from osprey.worker.lib.snowflake import Snowflake

pytestmark: List[Callable[[Any], Any]] = [
    pytest.mark.use_validators([ValidateCallKwargs, UniqueStoredNames]),
    pytest.mark.use_udf_registry(
        UDFRegistry.with_udfs(GetTimedeltaBucket, GetSnowflakeBucket, GetTimestampBucket, TimeDelta),
    ),
]


def test_get_snowflake_bucket(execute: ExecuteFunction) -> None:
    snowflake = Snowflake(119144447702335491)
    timestamp = int(snowflake.to_timestamp())  # Convert to integer seconds

    week_granularity = 604800
    day_granularity = 86400
    hour_granularity = 3600

    data = execute(
        f"""
        Week_bucket = GetSnowflakeBucket(snowflake={snowflake},granularity_seconds={week_granularity})
        Day_Bucket = GetSnowflakeBucket(snowflake={snowflake},granularity_seconds={day_granularity})
        Hour_Bucket = GetSnowflakeBucket(snowflake={snowflake},granularity_seconds={hour_granularity})
        """
    )
    assert data == {
        'Week_bucket': (timestamp // week_granularity) * week_granularity,
        'Day_Bucket': (timestamp // day_granularity) * day_granularity,
        'Hour_Bucket': (timestamp // hour_granularity) * hour_granularity,
    }


def test_get_timedelta_bucket(execute: ExecuteFunction) -> None:
    data = execute(
        """
        Week_bucket = GetTimedeltaBucket(timedelta=TimeDelta(seconds=1136715),granularity_seconds=604800)
        Day_Bucket = GetTimedeltaBucket(timedelta=TimeDelta(seconds=1136715),granularity_seconds=86400)
        Hour_Bucket = GetTimedeltaBucket(timedelta=TimeDelta(seconds=1136715),granularity_seconds=3600)
        """
    )
    assert data == {
        'Week_bucket': 604800,  # 1 week
        'Day_Bucket': 1123200,  # 13 days
        'Hour_Bucket': 1134000,  # 315 hours
    }


def test_get_timestamp_bucket(execute: ExecuteFunction) -> None:
    data = execute(
        """
        Week_bucket = GetTimestampBucket(timestamp=1448476649,granularity_seconds=604800)
        Day_Bucket = GetTimestampBucket(timestamp=1448476649,granularity_seconds=86400)
        Hour_Bucket = GetTimestampBucket(timestamp=1448476649,granularity_seconds=3600)
        """
    )
    assert data == {
        'Week_bucket': 1447891200,  # 11/19/2015 0:00:00
        'Day_Bucket': 1448409600,  # 11/25/2015 0:00:00
        'Hour_Bucket': 1448474400,  # 11/25/2015 18:00:00
    }


def test_get_timestamp_bucket_invalid_granularity(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation('GetTimestampBucket(timestamp=1448476649,granularity_seconds=0)')


def test_get_timedelta_bucket_invalid_granularity(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation('GetTimedeltaBucket(timedelta=TimeDelta(seconds=1136715),granularity_seconds=0)')


def test_get_snowflake_bucket_invalid_granularity(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    with check_failure():
        run_validation('GetSnowflakeBucket(snowflake=119144447702335491,granularity_seconds=0)')
