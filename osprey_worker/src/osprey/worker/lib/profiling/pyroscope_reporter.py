from __future__ import annotations

import atexit
import logging
import os
import socket
import time
import types
from collections import defaultdict
from dataclasses import dataclass

import gevent
import greenlet
import requests
from typing_extensions import Literal

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StackFrame:
    module: str | None
    """Module name"""
    name: str | None
    """Function name"""
    line: int | None
    """Line number"""

    def __str__(self) -> str:
        """Returns a string representation of the stack frame"""
        return '{} - {}:{}'.format(self.module, self.name, self.line)


@dataclass(frozen=True)
class StackTrace:
    frames: tuple[StackFrame, ...]
    """Tuple of stack frames"""

    def __str__(self) -> str:
        return ';'.join(str(frame) for frame in reversed(self.frames))

    @staticmethod
    def from_frametype(frame: types.FrameType) -> StackTrace:
        frames = []
        f: types.FrameType | None = frame
        while f is not None:
            name = f.f_code.co_name
            module = f.f_globals.get('__name__', None)
            line = f.f_lineno

            # Skip importlib frames
            if not (module and module.startswith('importlib._bootstrap')):
                frames.append(StackFrame(module, name, line))

            f = f.f_back

        return StackTrace(tuple(frames))


# Valid report types for Pyroscope
# From: https://github.com/grafana/pyroscope/blob/main/pkg/ingester/pyroscope/ingest_adapter.go#L201
ReportType = (
    Literal['cpu']
    | Literal['wall']
    | Literal['inuse_objects']
    | Literal['inuse_space']
    | Literal['alloc_objects']
    | Literal['alloc_space']
    | Literal['goroutines']
    | Literal['mutex_count']
    | Literal['mutex_wait']
    | Literal['mutex_duration']
    | Literal['block_count']
    | Literal['block_duration']
    | Literal['itimer']
    | Literal['alloc_in_new_tlab_objects']
    | Literal['alloc_in_new_tlab_bytes']
    | Literal['alloc_outside_tlab_objects']
    | Literal['alloc_outside_tlab_bytes']
    | Literal['lock_count']
    | Literal['lock_duration']
    | Literal['live']
    | Literal['exceptions']
)


class Reporter(object):
    def __init__(
        self,
        sample_rate_hz: float,
        report_type: ReportType = 'cpu',
        report_interval: float = 5.0,
        spy_name: str = 'osprey_common.profiling',
        include_hostname: bool = True,
    ) -> None:
        self.spy_name = spy_name
        self.sample_rate_hz = sample_rate_hz
        self.report_type = report_type
        self.report_interval = report_interval

        self.tags = {
            'env': os.getenv('ENVIRONMENT') or os.getenv('DD_ENV'),
            'version': os.getenv('DD_VERSION'),
        }
        if include_hostname:
            self.tags['hostname'] = socket.gethostname()

        self.pyroscope_host = os.getenv('PYROSCOPE_HOST', 'localhost')
        self.pyroscope_port = os.getenv('PYROSCOPE_PORT', '4040')
        self.pyroscope_url = os.getenv('PYROSCOPE_URL', f'http://{self.pyroscope_host}:{self.pyroscope_port}/ingest')

        self._started = time.time()
        self._stack_counts_by_key: defaultdict[tuple[str | None, str | None], defaultdict[StackTrace, int]] = (
            defaultdict(lambda: defaultdict(int))
        )
        self._report_greenlet: gevent.Greenlet | None = None

    def add_tags(self, tags: dict[str, str]) -> None:
        self.tags.update(tags)

    def name(self, feature: str | None = None, endpoint: str | None = None) -> str:
        """Generates the application name string for Pyroscope, optionally including a feature tag."""
        application_name = os.getenv('DD_SERVICE', 'osprey-api')
        all_tags = self.tags.copy()
        if feature:
            all_tags['feature'] = feature
        if endpoint:
            all_tags['endpoint'] = endpoint
        tags_str = ','.join(f'{k}={v}' for k, v in all_tags.items() if v is not None)
        return f'{application_name}.{self.report_type}{{{tags_str}}}'

    def record(self, stack_trace: StackTrace, feature: str | None = None, endpoint: str | None = None) -> None:
        """Records a stack trace occurrence, optionally associated with a feature and endpoint."""
        self._stack_counts_by_key[(feature, endpoint)][stack_trace] += 1

    def record_with_value(
        self, stack_trace: StackTrace, value: int, feature: str | None = None, endpoint: str | None = None
    ) -> None:
        """Records a stack trace occurrence with a specific value, optionally associated with a feature and endpoint."""
        self._stack_counts_by_key[(feature, endpoint)][stack_trace] += value

    def _report_loop(self) -> None:
        while True:
            try:
                self.report()
                gevent.sleep(self.report_interval)
            except (gevent.GreenletExit, greenlet.GreenletExit):
                break
            except Exception:
                logger.exception('Unhandled exception in Pyroscope report loop')
                gevent.sleep(self.report_interval)

    def report(self) -> None:
        try:
            # Swap out the stack counts so we don't accumulate more between building the report and clearing after
            # sending
            # TODO: Handle losing data when we're not able to send
            stack_counts_by_key = self._stack_counts_by_key
            self._stack_counts_by_key = defaultdict(lambda: defaultdict(int))
            from_ = self._started
            until = self._started = time.time()

            if not stack_counts_by_key:
                return

            for (feature, endpoint), stack_counts in stack_counts_by_key.items():
                if not stack_counts:
                    continue

                data = '\n'.join(f'{stack_trace} {count}' for stack_trace, count in stack_counts.items())
                name = self.name(feature=feature, endpoint=endpoint)

                # https://grafana.com/docs/pyroscope/latest/reference-server-api/
                params: dict[str, str] = {
                    'name': name,
                    'from': str(from_),
                    'until': str(until),
                    'format': 'folded',
                    'sampleRate': str(self.sample_rate_hz),
                    'unit': 'samples',
                    'spyName': self.spy_name,
                }
                try:
                    response = requests.post(
                        self.pyroscope_url,
                        params=params,
                        data=data,
                        timeout=5.0,
                    )
                    response.raise_for_status()
                except requests.exceptions.RequestException as e:
                    logger.warning(f'Error reporting profiles for feature "{feature}" to Pyroscope: {e}')

        except Exception as e:
            logger.warning('Error preparing Pyroscope report data', exc_info=e)
            current_counts = stack_counts_by_key
            for (feature, endpoint), counts in current_counts.items():
                for stack, count in counts.items():
                    self._stack_counts_by_key[(feature, endpoint)][stack] += count

    def start(self) -> None:
        atexit.register(self.stop)
        self._report_greenlet = gevent.spawn(self._report_loop)

    def stop(self) -> None:
        if self._report_greenlet:
            self._report_greenlet.kill()
            # Report one last time just to get all the samples out
            self.report()

    def __del__(self) -> None:
        self.stop()
