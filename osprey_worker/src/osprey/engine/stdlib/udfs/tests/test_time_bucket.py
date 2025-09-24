from typing import Any, Callable, List

import pytest
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.udf.registry import UDFRegistry

from ....conftest import CheckFailureFunction, ExecuteFunction, RunValidationFunction
from ..time_bucket import GetSnowflakeBucket, GetTimedeltaBucket, GetTimestampBucket
from ..time_delta import TimeDelta

pytestmark: List[Callable[[Any], Any]] = [
    pytest.mark.use_validators([ValidateCallKwargs, UniqueStoredNames]),
    pytest.mark.use_udf_registry(
        UDFRegistry.with_udfs(GetTimedeltaBucket, GetSnowflakeBucket, GetTimestampBucket, TimeDelta),
    ),
]


def test_get_snowflake_bucket(execute: ExecuteFunction) -> None:
    data = execute(
        """
        Week_bucket = GetSnowflakeBucket(snowflake=119144447702335491,granularity_seconds=604800)
        Day_Bucket = GetSnowflakeBucket(snowflake=119144447702335491,granularity_seconds=86400)
        Hour_Bucket = GetSnowflakeBucket(snowflake=119144447702335491,granularity_seconds=3600)
        """
    )
    assert data == {
        'Week_bucket': 1447891200,  # 11/19/2015 0:00:00
        'Day_Bucket': 1448409600,  # 11/25/2015 0:00:00
        'Hour_Bucket': 1448474400,  # 11/25/2015 18:00:00
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
