from __future__ import annotations

import atexit
import logging
import signal
import types

from flask import request

from .feature import current_features
from .pyroscope_reporter import Reporter, StackTrace

sampler: Sampler | None = None

logger = logging.getLogger(__name__)


class Sampler(object):
    def __init__(
        self,
        sample_rate_hz: float = 10,
        report_interval: float = 5,
        include_hostname: bool = True,
    ):
        self.sample_rate_hz = sample_rate_hz
        self._interval = 1.0 / sample_rate_hz

        self._reporter = Reporter(
            sample_rate_hz, report_type='cpu', report_interval=report_interval, include_hostname=include_hostname
        )

    def start(self) -> None:
        try:
            signal.signal(signal.SIGVTALRM, self._sample)
        except ValueError:
            raise ValueError('Can only sample on the main thread')

        signal.setitimer(signal.ITIMER_VIRTUAL, self._interval)
        self._reporter.start()
        atexit.register(self.stop)

    def _get_current_feature(self) -> str | None:
        """Get the current features from tiering library."""
        features = current_features()
        if not features:
            return None

        # TODO: decide how to handle multiple features
        return features[0].value

    def _get_current_endpoint(self) -> str | None:
        """Get the current endpoint from the request."""
        try:
            return request.endpoint
        except Exception:
            return None

    def _sample(self, _signum: int, frame: types.FrameType | None) -> None:
        if frame:
            feature = self._get_current_feature()
            endpoint = self._get_current_endpoint()
            self._reporter.record(StackTrace.from_frametype(frame), feature=feature, endpoint=endpoint)

        signal.setitimer(signal.ITIMER_VIRTUAL, self._interval)

    def stop(self) -> None:
        signal.setitimer(signal.ITIMER_VIRTUAL, 0)
        self._reporter.stop()

    def __del__(self) -> None:
        self.stop()


def run_sampler() -> None:
    sampler = Sampler()
    sampler.start()


def run_sampler_with_params(sample_rate_hz: float, report_interval: float = 5, include_hostname: bool = True) -> None:
    sampler = Sampler(sample_rate_hz=sample_rate_hz, report_interval=report_interval, include_hostname=include_hostname)
    sampler.start()
