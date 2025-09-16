from datetime import timedelta

from osprey.engine.language_types.time_delta import TimeDeltaT

from ._prelude import ArgumentsBase, ConstExpr, ExecutionContext, UDFBase
from .categories import UdfCategories


class Arguments(ArgumentsBase):
    weeks: ConstExpr[int] = ConstExpr.for_default('weeks', 0)
    days: ConstExpr[int] = ConstExpr.for_default('days', 0)
    hours: ConstExpr[int] = ConstExpr.for_default('hours', 0)
    minutes: ConstExpr[int] = ConstExpr.for_default('minutes', 0)
    seconds: ConstExpr[int] = ConstExpr.for_default('seconds', 0)


class TimeDelta(UDFBase[Arguments, TimeDeltaT]):
    """Returns a TimeDeltaT for the specified time frame."""

    category = UdfCategories.DATETIME

    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> TimeDeltaT:
        return TimeDeltaT(
            timedelta(
                weeks=arguments.weeks.value,
                days=arguments.days.value,
                hours=arguments.hours.value,
                minutes=arguments.minutes.value,
                seconds=arguments.seconds.value,
            )
        )
