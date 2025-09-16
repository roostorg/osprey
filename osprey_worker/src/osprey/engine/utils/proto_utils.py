from datetime import datetime, timedelta

from google.protobuf.duration_pb2 import Duration
from google.protobuf.timestamp_pb2 import Timestamp


def datetime_to_timestamp(dt: datetime) -> Timestamp:
    timestamp = Timestamp()
    timestamp.FromDatetime(dt)
    return timestamp


def optional_datetime_to_timestamp(dt: datetime | None) -> Timestamp | None:
    if dt is None:
        return None

    return datetime_to_timestamp(dt)


def timedelta_to_duration(td: timedelta) -> Duration:
    duration = Duration()
    duration.FromTimedelta(td)
    return duration


def optional_timedelta_to_duration(td: timedelta | None) -> Duration | None:
    if td is None:
        return None

    return timedelta_to_duration(td)
