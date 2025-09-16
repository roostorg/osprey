from __future__ import absolute_import

import time

from osprey.worker.lib.backoff import Backoff


class ExpiringBackoffHandler(object):
    """
    Simplifies working with a Backoff if you want to have attempt count
    and time limit restrictions applied.
    """

    def __init__(self, backoff=None, max_attempts=10, time_limit_secs=10):
        self.backoff = backoff or Backoff()  # type: Backoff
        self._max_attempts = max_attempts  # type: int
        self.time_limit = time_limit_secs  # type: int
        self.start_time = time.time()  # type: float

    def _time_since_started(self):
        # type: () -> float
        return time.time() - self.start_time

    def fail(self):
        # type: () -> float
        return self.backoff.fail()

    def is_resumable(self):
        # type: () -> bool
        return self.backoff.fails < self._max_attempts and self.time_limit > self._time_since_started()
