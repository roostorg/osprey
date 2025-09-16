import threading
import time
from contextlib import contextmanager
from typing import Any, Iterator


class PeriodicExecutionYielder:
    """Periodically sleeps calling thread for `yield_time_sec` if it's been over `execution_time_sec` since last sleep.

    Do not set `execution_time_sec` too low (< 0.001 s) to protect from always yielding
    """

    def __init__(self, execution_time_sec: float, yield_time_sec: float) -> None:
        self.last_wait = time.perf_counter()
        self.yield_time_sec = yield_time_sec
        self.execution_time_sec = execution_time_sec

    def periodic_yield(self) -> None:
        cur_time = time.perf_counter()
        if cur_time > self.last_wait + self.execution_time_sec:
            time.sleep(self.yield_time_sec)
            self.last_wait = cur_time + self.yield_time_sec


_context_activated_thread_local = threading.local()


def _get_active_execution_yielder() -> Any:
    return getattr(_context_activated_thread_local, 'execution_yielder', None)


def maybe_periodic_yield() -> None:
    yielder = _get_active_execution_yielder()
    if yielder:
        yielder.periodic_yield()


@contextmanager
def periodic_execution_yield(on: bool = False, execution_time_ms: int = 5, yield_time_ms: int = 10) -> Iterator[None]:
    if _get_active_execution_yielder():
        raise Exception('Context periodic_execution_yield is already active')

    try:
        if on:
            _context_activated_thread_local.execution_yielder = PeriodicExecutionYielder(
                execution_time_sec=execution_time_ms / 1000.0, yield_time_sec=yield_time_ms / 1000.0
            )
        yield
    finally:
        _context_activated_thread_local.execution_yielder = None
