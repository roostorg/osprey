from gevent import sleep
from gevent.lock import RLock


class FairRLock(RLock):  # type: ignore
    """A RLock that is fair, and will allow lock acquisitions that were attempted first to be acquired,
    which is distinct from the way that RLock works in gevent, which will allow the greenlet that just released
    the lock to re-acquire it, even if there is another greenlet waiting to acquire the lock, thus starving all
    other lockers."""

    def acquire(self, blocking: bool = True, timeout: float | None = None) -> bool:
        if blocking and not self._block.locked() and self._block.linkcount():
            sleep(0)

        return super().acquire(blocking=blocking, timeout=timeout)
