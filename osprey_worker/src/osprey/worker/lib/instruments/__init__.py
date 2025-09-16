from __future__ import absolute_import, annotations, print_function

import contextlib
import os
import re
from types import TracebackType
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Pattern, Sequence, Type, Union

from datadog.dogstatsd.base import DogStatsd

UNDERSCORE_REPLACEMENT_CHARACTERS: Pattern[str] = re.compile(r'-')
INVALID_METRIC_NAME_CHARACTERS: Pattern[str] = re.compile(r'[^a-zA-Z0-9._]')
MULTIPLE_DOTS: Pattern[str] = re.compile(r'\.{2,}')
DEFAULT_SAMPLE_RATE = 1.0

if TYPE_CHECKING:
    from flask import Flask
    from osprey.worker.lib.instruments.types import SupportsRichComparisonT, T


def percentile(
    unsorted_list: Sequence[SupportsRichComparisonT], requested_percentile: Union[int, float]
) -> Optional[SupportsRichComparisonT]:
    """Return the value at a percentile

    Given an unsorted list as input, this returns the value at the requested
    percentile. If you are requesting many percentiles and don't want to
    have to sort the list N times, use percentile_sorted.
    """
    if not unsorted_list:
        return None
    sorted_list = sorted(unsorted_list)
    return percentile_sorted(sorted_list, requested_percentile)


def percentile_sorted(sorted_list: Sequence[T], requested_percentile: Union[int, float]) -> Optional[T]:
    """Return the value at a percentile

    Given a sorted list, return the value at the requested percentile. If your
    list is unsorted, this function will return nonsense.
    """
    pos = max(0, int(len(sorted_list) * (requested_percentile / 100.0)) - 1)
    return sorted_list[pos]


def sanitize_metric_name(name: str) -> str:
    """Sanitizes a metric name by removing invalid characters"""
    name = re.sub(UNDERSCORE_REPLACEMENT_CHARACTERS, '_', name)
    name = re.sub(INVALID_METRIC_NAME_CHARACTERS, '.', name)
    return re.sub(MULTIPLE_DOTS, '.', name)


class _DogStatsd(DogStatsd):
    def __init__(
        self,
        host: str = 'localhost',
        port: int = 8125,
        max_buffer_size: int = 50,
        constant_tags: Optional[List[str]] = None,
        use_ms: bool = False,
    ):
        # If not None, this will override host/port
        socket_path = os.getenv('DOGSTATSD_SOCKET_PATH')
        host = os.getenv('DD_AGENT_HOST', host)
        port = int(os.getenv('DD_AGENT_PORT', port))

        if constant_tags is None:
            constant_tags = []

        # Use the DD_SERVICE as the `role` tag if it's present, as it fulfills the same purpose in K8s as the role tag
        # from Salt days
        role = os.getenv('DD_SERVICE')
        if role is not None:
            constant_tags.append('role:{}'.format(role))

        super().__init__(
            socket_path=socket_path,
            host=host,
            port=port,
            max_buffer_size=max_buffer_size,  # type:ignore
            constant_tags=constant_tags,
            use_ms=use_ms,
        )
        self.prefix = None
        self.debug = False

    def init_app(self, app: Flask) -> None:
        self.prefix = app.config.get('DATADOG_PREFIX')
        self.debug = app.debug

    def _report(
        self,
        metric: str,
        metric_type: str,
        value: float,
        tags: Optional[Union[Dict[str, str], List[str]]],
        sample_rate: Optional[float],
        timestamp: Optional[int] = None,
    ) -> None:
        if self.debug:
            return
        if self.prefix is not None:
            metric = '%s.%s' % (self.prefix, metric)

        if sample_rate is None:
            sample_rate = getattr(self, 'default_sample_rate', DEFAULT_SAMPLE_RATE)

        super()._report(metric, metric_type, value, self._transform_tags(tags), sample_rate)

    def gauge(
        self,
        metric: str,
        value: float = 1,
        tags: Optional[Union[Dict[str, str], List[str]]] = None,
        sample_rate: Optional[float] = None,
    ) -> None:
        """
        Update a gauge value.

        FYI Every unique set of metric name + tags is counted as a separate metric we get billed for
        """

        # Unfortunately, gauge is already typed in the base class, so we must do the transformation
        # ahead of the call to _report in order to ensure our tags are of the proper type
        super().gauge(metric, value, self._transform_tags(tags), sample_rate)

    def increment(
        self,
        metric: str,
        value: float = 1,
        tags: Optional[Union[Dict[str, str], List[str]]] = None,
        sample_rate: Optional[float] = None,
    ) -> None:
        """
        Increment a metric by the given amount.

        FYI Every unique set of metric name + tags is counted as a separate metric we get billed for
        """
        # Unfortunately, increments is already typed in the base class, so we must do the transformation
        # ahead of the call to _report in order to ensure our tags are of the proper type
        super().increment(metric, value, self._transform_tags(tags), sample_rate)

    def distribution(
        self,
        metric: str,
        value: float,
        tags: Optional[Union[Dict[str, str], List[str]]] = None,
        sample_rate: Optional[float] = None,
    ) -> None:
        """
        Distributions are a metric type that aggregate values sent from multiple hosts
        during a flush interval to measure statistical distributions across your entire
        infrastructure.Unlike histograms which aggregate on the Agent-side, global
        distributions send all raw data collected during the flush interval and the
        aggregation occurs server-side

        NOTE: This is an expensive metric to compute, so consider using sample_rate.
        """
        super().distribution(metric, value, self._transform_tags(tags), sample_rate)

    def timing(
        self,
        metric: str,
        value: float,
        tags: Optional[Union[Dict[str, str], List[str]]] = None,
        sample_rate: Optional[float] = None,
    ) -> None:
        super().timing(metric, value, self._transform_tags(tags), sample_rate)

    def event(
        self,
        title: str,
        message: str,
        alert_type: Optional[str] = None,
        aggregation_key: Any = None,
        source_type_name: Any = None,
        date_happened: Any = None,
        priority: Any = None,
        tags: Optional[List[str]] = None,
        hostname: Optional[str] = None,
    ) -> None:
        """
        Log an event that can be overlayed over Data Dog's graphs
        """
        super().event(
            title=title,
            message=message,
            alert_type=alert_type,
            aggregation_key=aggregation_key,
            source_type_name=source_type_name,
            date_happened=date_happened,
            priority=priority,
            tags=tags,
            hostname=hostname,
        )

    @staticmethod
    def _transform_tags(tags: Optional[Union[Dict[str, str], List[str]]] = None) -> Optional[List[str]]:
        if tags is None:
            return None
        if isinstance(tags, dict):
            return [f'{k}:{v}' for k, v in tags.items()]
        return tags


metrics = _DogStatsd()


class concurrency(contextlib.ContextDecorator):
    """A decorator for tracking concurrent calls of a function as a Gauge

    NOTE: If this is used on a recursive function, the metrics will likely be garbage
    """

    def __init__(self, metric_name: str, metric_tags: Optional[List[str]] = None):
        self.metric_name = metric_name
        self.metric_tags = metric_tags
        self._count = 0

    def _emit_metric(self) -> None:
        metrics.gauge(self.metric_name, self._count, tags=self.metric_tags)

    def __enter__(self) -> None:
        self._count += 1
        self._emit_metric()

    def __exit__(
        self, type: Optional[Type[BaseException]], value: Optional[BaseException], traceback: Optional[TracebackType]
    ) -> None:
        self._count -= 1
        self._emit_metric()
