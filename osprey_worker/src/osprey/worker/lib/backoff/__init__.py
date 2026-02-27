from __future__ import absolute_import

import random


class Backoff(object):
    """
    A class that manages an exponential backoff.
    """

    def __init__(self, min_delay: float = 0.5, max_delay: float | None = None, jitter: bool = True):
        self._min = min_delay
        if max_delay is None:
            max_delay = min_delay * 10

        self._max = max_delay
        self._jitter = jitter

        self._current = self._min
        self._fails = 0

    @property
    def fails(self) -> int:
        """
        Return the number of failures.
        """
        return self._fails

    @property
    def current(self) -> float:
        """
        Current backoff value in seconds.
        """
        return self._current

    def succeed(self):
        """
        Resets the backoff.
        """
        self._fails = 0
        self._current = self._min

    def fail(self) -> float:
        """
        Increments the backoff and returns the delay to wait.
        """
        self._current = self._min * (2**self._fails)
        self._fails += 1

        if self._max:
            self._current = min(self._current, self._max)

        if self._jitter:
            half = self._current / 2.0
            self._current = half + (half * random.random())

        self._current = round(self._current, 2)
        return self._current
