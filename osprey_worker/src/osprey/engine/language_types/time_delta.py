import datetime
from dataclasses import dataclass

from osprey.engine.utils.types import add_slots

from .post_execution_convertible import PostExecutionConvertible


@add_slots
@dataclass
class TimeDeltaT(PostExecutionConvertible[float]):
    timedelta: datetime.timedelta

    def to_post_execution_value(self) -> float:
        # We can't convert timedelta's to JSON, so we store it as a number of seconds
        return self.timedelta.total_seconds()

    @classmethod
    def inner_from_optional(cls, maybe_time_delta: 'TimeDeltaT | None') -> datetime.timedelta | None:
        if maybe_time_delta is None:
            return None

        return maybe_time_delta.timedelta
