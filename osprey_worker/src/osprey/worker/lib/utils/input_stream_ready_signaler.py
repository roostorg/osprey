import random

import gevent


class InputStreamReadySignaler:
    def __init__(self) -> None:
        self._event = gevent.event.Event()
        self._event.set()

    def should_pause_input_stream(self) -> bool:
        return not self._event.is_set()

    def wait_until_resume(self) -> None:
        self._event.wait()

    def pause_input_stream(self) -> None:
        gevent.sleep(random.uniform(0, 600))  # random jitter to make sure not all workers pause at the same time
        self._event.clear()

    def resume_input_stream(self) -> None:
        self._event.set()

    @staticmethod
    def wait_for_input_stream_to_pause() -> None:
        gevent.idle()
