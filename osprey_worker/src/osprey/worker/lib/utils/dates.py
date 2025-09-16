import time
from datetime import datetime
from typing import Union

import pytz
from dateutil import parser
from pytz import UTC

OSPREY_EPOCH = 1420070400000
UNIX_EPOCH = datetime(year=1970, month=1, day=1, tzinfo=UTC)


def parse_go_timestamp(go_timestamp: str) -> datetime:
    """Parse a json-formatted golang time.Time into microseconds since the unix epoch.

    Format is iso rfc3339 with nanoseconds localized to UTC"""
    return parser.parse(go_timestamp)


class SnowflakeParseError(ValueError):
    pass


def snowflake_to_datetime(snowflake: Union[str, int]) -> datetime:
    """Extract the timestamp from a Snowflake and return a UTC datetime."""
    time_portion = int(snowflake) >> 22
    if time_portion == 0:
        # Probably not a real snowflake
        raise SnowflakeParseError(f"Doesn't look like a valid Snowflake: {snowflake}")
    return datetime.fromtimestamp((time_portion + OSPREY_EPOCH) / 1000.0, pytz.UTC)


def to_unix_timestamp(timestamp: datetime) -> float:
    """Convert a datetime into a unix timestamp."""
    return (timestamp - UNIX_EPOCH).total_seconds()


def time_ms() -> int:
    return time.time_ns() // 1000000
