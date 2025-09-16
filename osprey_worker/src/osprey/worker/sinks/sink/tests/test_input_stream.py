from typing import Iterator, List

import gevent
import gevent.event
from osprey.worker.sinks.sink.input_stream import BaseInputStream


def test_does_not_lock_non_generator_iterator() -> None:
    waiting_events: List[gevent.event.Event] = []

    class MyIterator(Iterator[int]):
        def __init__(self) -> None:
            super().__init__()
            self._next_num = 1

        def __next__(self) -> int:
            event = gevent.event.Event()
            waiting_events.append(event)
            event.wait()
            waiting_events.remove(event)

            next_num = self._next_num
            self._next_num += 1
            return next_num

    class NonGeneratorStream(BaseInputStream[int]):
        def _gen(self) -> Iterator[int]:
            return MyIterator()

    stream = NonGeneratorStream()

    g1 = gevent.spawn(lambda: next(stream))
    g2 = gevent.spawn(lambda: next(stream))

    gevent.idle()

    assert len(waiting_events) == 2

    for waiting_event in waiting_events:
        waiting_event.set()
    gevent.idle()

    assert len(waiting_events) == 0
    assert g1.get() == 1
    assert g2.get() == 2


def test_does_lock_generator_iterator() -> None:
    waiting_events: List[gevent.event.Event] = []

    class GeneratorStream(BaseInputStream[int]):
        def _gen(self) -> Iterator[int]:
            next_num = 1
            while True:
                event = gevent.event.Event()
                waiting_events.append(event)
                event.wait()
                waiting_events.remove(event)

                my_next_num = next_num
                next_num += 1
                yield my_next_num

    stream = GeneratorStream()

    g1 = gevent.spawn(lambda: next(stream))
    g2 = gevent.spawn(lambda: next(stream))

    gevent.idle()

    assert len(waiting_events) == 1

    for waiting_event in waiting_events:
        waiting_event.set()
    gevent.idle()

    assert len(waiting_events) == 1

    for waiting_event in waiting_events:
        waiting_event.set()
    gevent.idle()

    assert len(waiting_events) == 0

    assert g1.get() == 1
    assert g2.get() == 2
