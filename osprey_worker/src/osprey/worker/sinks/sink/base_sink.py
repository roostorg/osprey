import abc
import logging
from concurrent.futures import ProcessPoolExecutor
from typing import Any, Callable, Dict, List, Optional

import gevent

LOGGER = logging.getLogger(__name__)


class BaseSink(abc.ABC):
    """Represents an object that consumes objects from some sort of source."""

    @abc.abstractmethod
    def run(self) -> None:
        """Run this sink."""
        raise NotImplementedError

    @abc.abstractmethod
    def stop(self) -> None:
        """Stop this sink."""
        raise NotImplementedError


class PooledSink(BaseSink):
    """
    An implementation of the base sink that spawns a number of workers to concurrently execute sinks
    as produced by the given factory.
    """

    def __init__(self, factory: Callable[[], BaseSink], num_workers: int):
        self._num_workers = num_workers
        self._factory = factory
        self._children_sinks: List[BaseSink] = []  # set, never append

    def run(self) -> None:
        self._children_sinks = [self._factory() for _ in range(self._num_workers)]

        children = [gevent.spawn(sink.run) for sink in self._children_sinks]
        try:
            gevent.joinall(children)
        except gevent.GreenletExit:
            gevent.killall(children)

    def stop(self) -> None:
        for self._child_sink in self._children_sinks:
            self._child_sink.stop()


class ProcessPoolSink(BaseSink):
    """
    An implementation of the base sink that uses a process pool to concurrently execute sinks
    as produced by the given factory.
    """

    def __init__(
        self,
        factory: Callable[..., BaseSink],
        num_workers: int,
        args: Optional[Dict[str, Any]] = None,
    ):
        self._num_workers = num_workers
        self._factory = factory
        self._args = args if args is not None else {}
        self._executor: Optional[ProcessPoolExecutor] = None

    def run(self) -> None:
        """Run the sink workers in a process pool."""
        with ProcessPoolExecutor(max_workers=self._num_workers) as executor:
            self._executor = executor

            futures = [executor.submit(self._run_sink, self._factory, self._args) for _ in range(self._num_workers)]
            try:
                for future in futures:
                    future.result()
            except KeyboardInterrupt:
                self.stop()
                raise
            finally:
                self._executor = None

    @staticmethod
    def _run_sink(factory: Callable[..., BaseSink], args: Dict[str, Any]) -> None:
        try:
            sink = factory(**args)  # Instantiate the sink within the worker
            sink.run()
        except Exception as e:
            LOGGER.exception(f'Error in sink worker: {e}')

    def stop(self) -> None:
        """Stop all sink workers and shut down the process pool."""
        if self._executor is not None:
            self._executor.shutdown(wait=True)
