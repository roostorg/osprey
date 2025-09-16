from datetime import timedelta, timezone

from dateutil import parser
from osprey.engine.language_types.time_delta import TimeDeltaT

from ._prelude import ArgumentsBase, ExecutionContext, UDFBase
from .categories import UdfCategories

_ZERO_TIMEDELTA = timedelta()


class Arguments(ArgumentsBase):
    timestamp: str


def convert_js_timestamp_to_python(timestamp: str) -> str:
    # Date is str len 10, time is HH:MM:SS.ffffff (15), includes the 'T'
    timestamp = timestamp[:26] + '+00:00'
    return timestamp


class TimeSince(UDFBase[Arguments, TimeDeltaT]):
    """Returns a `TimeDeltaT` representing the amount of time between the provided timestamp and now."""

    category = UdfCategories.DATETIME

    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> TimeDeltaT:
        timestamp = arguments.timestamp
        parsed = parser.parse(timestamp).replace(tzinfo=timezone.utc)
        return TimeDeltaT(
            max(execution_context.get_action_time().replace(tzinfo=timezone.utc) - parsed, _ZERO_TIMEDELTA)
        )
