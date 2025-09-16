import time

from ..periodic_execution_yielder import PeriodicExecutionYielder


# possibly need to mock out sleep calls if we run into unit test issues
def test_periodic_yield() -> None:
    execution_time_sec = 0.001
    yield_time_sec = 0.002
    execution_yielder = PeriodicExecutionYielder(execution_time_sec, yield_time_sec)

    cur_time = time.perf_counter()
    execution_yielder.periodic_yield()
    assert time.perf_counter() - cur_time < yield_time_sec
    time.sleep(execution_time_sec)
    cur_time = time.perf_counter()
    execution_yielder.periodic_yield()
    assert time.perf_counter() - cur_time >= yield_time_sec
