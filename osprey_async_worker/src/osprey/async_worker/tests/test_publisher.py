"""Tests for AsyncPubSubPublisher."""

from unittest.mock import MagicMock, patch

from google.api_core.exceptions import NotFound
from osprey.async_worker.lib.publisher import _PUBLISH_RETRY, AsyncPubSubPublisher


def _make_publisher():
    """Return an AsyncPubSubPublisher with a mocked PublisherClient."""
    with patch('osprey.async_worker.lib.publisher.pubsub_v1.PublisherClient'):
        publisher = AsyncPubSubPublisher(project_id='proj', topic_id='topic')
    publisher._client = MagicMock()
    return publisher


def _make_future(result=None, exc=None):
    future = MagicMock()
    if exc is not None:
        future.result.side_effect = exc
    else:
        future.result.return_value = result
    return future


@patch('osprey.async_worker.lib.publisher.metrics')
def test_single_attempt_success(mock_metrics):
    publisher = _make_publisher()
    publisher._client.publish.return_value = _make_future(result='msg-id-1')

    publisher._sync_flush([b'hello'])

    publisher._client.publish.assert_called_once_with(publisher._topic_path, b'hello', retry=_PUBLISH_RETRY)
    mock_metrics.increment.assert_any_call('async_pubsub_publisher.publish.success', tags=publisher._metric_tags)
    failure_calls = [c for c in mock_metrics.increment.call_args_list if 'failure' in c[0][0]]
    assert failure_calls == []


@patch('osprey.async_worker.lib.publisher.metrics')
def test_permanent_failure_metric_fires(mock_metrics):
    publisher = _make_publisher()
    exc = NotFound('topic not found')
    publisher._client.publish.return_value = _make_future(exc=exc)

    publisher._sync_flush([b'data'])

    failure_calls = [c for c in mock_metrics.increment.call_args_list if 'failure' in c[0][0]]
    assert len(failure_calls) == 1
    assert failure_calls[0][0][0] == 'async_pubsub_publisher.publish.failure'
    assert f'error:{exc.__class__.__name__}' in failure_calls[0][1]['tags']
