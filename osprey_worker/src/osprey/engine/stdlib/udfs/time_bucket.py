from osprey.engine.language_types.time_delta import TimeDeltaT
from osprey.worker.lib.snowflake import Snowflake

from ._prelude import ArgumentsBase, ConstExpr, ExecutionContext, UDFBase, ValidationContext
from .categories import UdfCategories


class TimedeltaArguments(ArgumentsBase):
    timedelta: TimeDeltaT
    granularity_seconds: ConstExpr[int]


class GetTimedeltaBucket(UDFBase[TimedeltaArguments, int]):
    """Returns number of seconds that is rounded to the nearest granularity from a timedelta"""

    category = UdfCategories.DATETIME

    def __init__(self, validation_context: 'ValidationContext', arguments: TimedeltaArguments):
        super().__init__(validation_context, arguments)

        if arguments.granularity_seconds.value < 3600:
            validation_context.add_error(
                'invalid `granularity_seconds`',
                span=arguments.granularity_seconds.argument_span,
                hint='granularity_seconds can not be less than 3600 (1 hour)',
            )

    def execute(self, execution_context: ExecutionContext, arguments: TimedeltaArguments) -> int:
        timedelta_seconds = int(arguments.timedelta.to_post_execution_value())
        return timedelta_seconds - (timedelta_seconds % arguments.granularity_seconds.value)


class TimestampArguments(ArgumentsBase):
    timestamp: int
    granularity_seconds: ConstExpr[int]


class GetTimestampBucket(UDFBase[TimestampArguments, int]):
    """Returns a unix timestamp that is rounded to the nearest granularity from a unix timestamp"""

    category = UdfCategories.DATETIME

    def __init__(self, validation_context: 'ValidationContext', arguments: TimestampArguments):
        super().__init__(validation_context, arguments)
        if arguments.granularity_seconds.value < 3600:
            validation_context.add_error(
                'invalid `granularity_seconds`',
                span=arguments.granularity_seconds.argument_span,
                hint='granularity_seconds can not be less than 3600 (1 hour)',
            )

    def execute(self, execution_context: ExecutionContext, arguments: TimestampArguments) -> int:
        return arguments.timestamp - (arguments.timestamp % arguments.granularity_seconds.value)


class SnowflakeArguments(ArgumentsBase):
    snowflake: int
    granularity_seconds: ConstExpr[int]


class GetSnowflakeBucket(UDFBase[SnowflakeArguments, int]):
    """Returns a unix timestamp that is rounded to the nearest granularity from a snowflake"""

    category = UdfCategories.DATETIME

    def __init__(self, validation_context: 'ValidationContext', arguments: SnowflakeArguments):
        super().__init__(validation_context, arguments)
        if arguments.granularity_seconds.value < 3600:
            validation_context.add_error(
                'invalid `granularity_seconds`',
                span=arguments.granularity_seconds.argument_span,
                hint='granularity_seconds can not be less than 3600 (1 hour)',
            )

    def execute(self, execution_context: ExecutionContext, arguments: SnowflakeArguments) -> int:
        # Convert the inputted snowflake into unix time
        snowflake_timestamp_seconds = int(Snowflake(arguments.snowflake).to_timestamp())
        return snowflake_timestamp_seconds - (snowflake_timestamp_seconds % arguments.granularity_seconds.value)
