from collections.abc import Callable
from typing import Any

import pytest
from datadog.dogstatsd.base import DogStatsd
from osprey.worker.lib.instruments import metrics
from pytest_mock import MockFixture


@pytest.mark.parametrize(
    'tags',
    (
        pytest.param(['test_tag_1:test_value_1', 'test_tag_2:test_value_2'], id='list'),
        pytest.param({'test_tag_1': 'test_value_1', 'test_tag_2': 'test_value_2'}, id='dict'),
    ),
)
def test_report_accepts_dict_or_list_tags(tags: dict[str, str] | list[str], mocker: MockFixture) -> None:
    mock = mocker.patch('datadog.dogstatsd.base.DogStatsd._report', autospec=True)

    metrics._report('test.metric', 'c', 1, tags, 1)
    mock.assert_called_once_with(
        metrics, 'test.metric', 'c', 1, ['test_tag_1:test_value_1', 'test_tag_2:test_value_2'], 1, 0
    )


def test_report_forwards_extra_base_class_arguments(mocker: MockFixture) -> None:
    # Newer datadog releases call _report with extra arguments (e.g. `sampling`, `cardinality`).
    # Not autospec'd, so this holds whichever datadog version is installed.
    mock = mocker.patch('datadog.dogstatsd.base.DogStatsd._report')

    metrics._report('test.metric', 'h', 1, {'a': 'b'}, 0.5, 0, False, cardinality='low')
    mock.assert_called_once_with('test.metric', 'h', 1, ['a:b'], 0.5, 0, False, cardinality='low')


@pytest.mark.parametrize(
    'emit',
    (
        pytest.param(lambda: metrics.gauge('test.gauge', 1, tags={'a': 'b'}), id='gauge'),
        pytest.param(lambda: metrics.increment('test.count', tags={'a': 'b'}), id='increment'),
        pytest.param(lambda: metrics.decrement('test.count', tags=['a:b']), id='decrement'),
        pytest.param(lambda: metrics.histogram('test.histogram', 1, tags=['a:b']), id='histogram'),
        pytest.param(lambda: metrics.distribution('test.distribution', 1, tags={'a': 'b'}), id='distribution'),
        pytest.param(lambda: metrics.timing('test.timing', 1, tags={'a': 'b'}), id='timing'),
        pytest.param(lambda: metrics.set('test.set', 1, tags=['a:b']), id='set'),
    ),
)
def test_public_metric_methods_reach_report(emit: Callable[[], None], mocker: MockFixture) -> None:
    # Goes through the real DogStatsd methods (and client-side aggregation) so that a mismatch between
    # our _report override and how the installed datadog version calls it fails here.
    reported: list[tuple[Any, ...]] = []
    mocker.patch(
        'datadog.dogstatsd.base.DogStatsd._report',
        side_effect=lambda metric, metric_type, value, tags, *args, **kwargs: reported.append((metric, tags)),
    )
    metrics.flush_aggregated_metrics()
    reported.clear()

    emit()
    metrics.flush_aggregated_metrics()

    # Filter by name: the client's background flush thread or other code may report unrelated metrics meanwhile.
    ours = [tags for metric, tags in reported if metric.startswith('test.')]
    assert ours == [['a:b']]


@pytest.mark.parametrize('method', ('gauge', 'increment', 'distribution', 'timing'))
def test_cardinality_is_forwarded_only_when_set(method: str, mocker: MockFixture) -> None:
    mock = mocker.patch.object(DogStatsd, method)

    getattr(metrics, method)('test.metric', 1, {'a': 'b'}, 1)
    mock.assert_called_once_with('test.metric', 1, ['a:b'], 1)

    mock.reset_mock()
    getattr(metrics, method)('test.metric', 1, {'a': 'b'}, 1, cardinality='high')
    mock.assert_called_once_with('test.metric', 1, ['a:b'], 1, cardinality='high')


def test_event_cardinality_is_forwarded_only_when_set(mocker: MockFixture) -> None:
    mock = mocker.patch.object(DogStatsd, 'event')

    metrics.event('title', 'message')
    assert 'cardinality' not in mock.call_args.kwargs

    metrics.event('title', 'message', cardinality='low')
    assert mock.call_args.kwargs['cardinality'] == 'low'
