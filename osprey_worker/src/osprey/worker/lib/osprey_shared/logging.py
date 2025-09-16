import logging
from datetime import datetime, timedelta
from random import SystemRandom
from typing import Optional


class RollingCounter:
    """
    A circular buffer that keeps track of a rolling sum of counts per defined interval.

    The `buffer_capacity` defines the size of the buffer, and the `milliseconds_per_index`
    defines the interval at which the buffer index is incremented. Multiply the two together
    to get the total time window that the rolling counter covers.

    This class has tests written over in `osprey/osprey_lib/tests/test_logging.py`.
    """

    index = 0

    def __init__(self, buffer_capacity: int, milliseconds_per_index: int):
        self.buffer_capacity = buffer_capacity
        self.milliseconds_per_index = milliseconds_per_index
        self.index_timestamp = datetime.now() + timedelta(milliseconds=milliseconds_per_index)
        self.circle_buffer = [0] * self.buffer_capacity

    def _progress_buffer_index_if_time_elapsed(self):
        # Check if the index timestamp has passed to progress the buffer
        if self.index_timestamp < datetime.now():
            # If more time has elapsed than the interval, clear the respective cells
            # & increment the index by the number of indicies that have elapsed
            num_indexes_elapsed = min(
                self.buffer_capacity,
                # Get the excess time passed since index_timestamp and divide it by milliseconds_per_index to get
                # the number of indexes that have elapsed. We cap this at the size of the buffer since that would be a full clear ^^
                int((datetime.now() - self.index_timestamp) / timedelta(milliseconds=self.milliseconds_per_index)) + 1,
            )
            for _ in range(num_indexes_elapsed):
                self.index = (self.index + 1) % self.buffer_capacity
                self.circle_buffer[self.index] = 0
            self.index_timestamp = datetime.now() + timedelta(milliseconds=self.milliseconds_per_index)

    def increment(self):
        self._progress_buffer_index_if_time_elapsed()
        self.circle_buffer[self.index] += 1

    def get_count(self):
        self._progress_buffer_index_if_time_elapsed()
        return sum(self.circle_buffer)


class DynamicLogSampler(logging.Filter):
    random_generator = SystemRandom()
    num_increments_per_recalculation = 25

    def __init__(self, target_max_logs_per_minute: int, target_max_errors_per_minute: int, name: str = '') -> None:
        """
        Filters logs based on the number of logs per minute.
        At less than half the `target_max_logs_per_minute` target, the probability of logging is 100%.
        Once half way to `target_max_logs_per_minute` is reached, the probability begins to drop to keep
        the rate at or below the target.

        Logging is restored as the rate of logs drops below half the target maximum.

        NOTE: Error logs operate under their own SEPARATE `target_max_errors_per_minute` limit.
        """
        super().__init__(name)
        self.target_max_logs_per_minute = target_max_logs_per_minute
        self.target_max_errors_per_minute = target_max_errors_per_minute
        self.probability: float = 1.0
        self.error_probability: float = 1.0
        # A 1 minute rolling counter for non-error logs
        self.rolling_log_counter = RollingCounter(buffer_capacity=60, milliseconds_per_index=1000)
        # A 1 minute rolling counter for *errors* specifically.
        # This is so info log spam doesn't prevent us from getting important errors,
        # and vice versa ^^
        self.rolling_error_counter = RollingCounter(buffer_capacity=60, milliseconds_per_index=1000)
        #
        self.num_increments_since_log_recalculation: int = 0
        self.num_increments_since_error_recalculation: int = 0

    def _process_record(self, record: logging.LogRecord):
        if record.levelno >= logging.ERROR:
            self._process_error_record(record)
        else:
            self._process_log_record(record)

    def _process_log_record(self, record: Optional[logging.LogRecord] = None):
        self.rolling_log_counter.increment()
        self.num_increments_since_log_recalculation += 1
        # Only recalculate once per num_increments_per_recalculation log msgs to prevent excessive recalculation
        if self.num_increments_since_log_recalculation >= self.num_increments_per_recalculation:
            self._recalculate_probability()

    def _process_error_record(self, record: Optional[logging.LogRecord] = None):
        self.rolling_error_counter.increment()
        self.num_increments_since_error_recalculation += 1
        # Only recalculate once per num_increments_since_error_recalculation errors to prevent excessive recalculation
        if self.num_increments_since_error_recalculation >= self.num_increments_per_recalculation:
            self._recalculate_error_probability()

    def _recalculate_probability(self):
        self.num_increments_since_log_recalculation = 0
        current_logs_per_min = self.rolling_log_counter.get_count()
        if current_logs_per_min > self.target_max_logs_per_minute / 2:
            self.probability = max(0, min(1, 1 - int(current_logs_per_min / max(self.target_max_logs_per_minute, 1))))
        else:
            self.probability = 1.0

    def _recalculate_error_probability(self):
        self.num_increments_since_error_recalculation = 0
        current_errors_per_min = self.rolling_error_counter.get_count()
        if current_errors_per_min > self.target_max_errors_per_minute / 2:
            self.error_probability = max(
                0, min(1, 1 - int(current_errors_per_min / max(self.target_max_errors_per_minute, 1)))
            )
        else:
            self.error_probability = 1.0

    def _should_log(self, record: Optional[logging.LogRecord] = None) -> bool:
        if record is not None and record.levelno >= logging.ERROR:
            if self.error_probability <= 0:
                return False
            return self.error_probability == 1 or self.random_generator.random() < self.error_probability
        else:
            if self.probability <= 0:
                return False
            return self.probability == 1 or self.random_generator.random() < self.probability

    def filter(self, record):
        self._process_record(record)
        return self._should_log(record)


# We recycle the logger instance across all loggers so this is a per service limit.
# This limit is agnostic of the log level at the moment.
# TODO: this probably should live in the singleton file
DEFAULT_DYNAMIC_LOG_SAMPLER = DynamicLogSampler(target_max_logs_per_minute=30, target_max_errors_per_minute=120)


def get_logger(
    name: Optional[str] = None, dynamic_log_sampler: Optional[DynamicLogSampler] = DEFAULT_DYNAMIC_LOG_SAMPLER
) -> logging.Logger:
    """
    Gets a dynamically sampled logger with a shared log rate limit across all loggers
    for this service.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if dynamic_log_sampler is not None:
        logger.addFilter(dynamic_log_sampler)

    return logger


def info_log_osprey_action(action_id: int, action_name: str, message: str):
    """
    Info logging for a percentage of actions based on action id.
    This method should be synced with the one in Osprey Coordinator.

    """
    # TODO: this method is called in multiple places and just swallows logging
    pass

    # # Edit this if you want to log different actions end-to-end in Osprey
    # if action_name != 'guild_member_updated':
    #     return

    # if action_id % 100000 == 0:
    #     logger.info(f'{{action_id={action_id}}} {{action_name={action_name}}} {message}')
