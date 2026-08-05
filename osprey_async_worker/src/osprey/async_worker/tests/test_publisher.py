"""Tests for AsyncPubSubPublisher."""

import asyncio
import threading
from unittest.mock import MagicMock, patch

import pytest
from google.api_core.exceptions import DeadlineExceeded, NotFound, RetryError
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

    retry_messages = publisher._sync_flush([b'data'])

    assert retry_messages == []
    failure_calls = [c for c in mock_metrics.increment.call_args_list if 'failure' in c[0][0]]
    assert len(failure_calls) == 1
    assert failure_calls[0][0][0] == 'async_pubsub_publisher.publish.failure'
    assert f'error:{exc.__class__.__name__}' in failure_calls[0][1]['tags']


def test_retry_policy_includes_observed_timeout_errors():
    assert _PUBLISH_RETRY._predicate(TimeoutError())
    assert _PUBLISH_RETRY._predicate(DeadlineExceeded('deadline exceeded'))


@patch('osprey.async_worker.lib.publisher.metrics')
def test_sync_flush_requeues_exhausted_transient_retry(mock_metrics):
    publisher = _make_publisher()
    publisher._client.publish.return_value = _make_future(
        exc=RetryError('deadline exceeded', DeadlineExceeded('retry me')),
    )

    retry_messages = publisher._sync_flush([b'retry'])

    assert retry_messages == [b'retry']


@patch('osprey.async_worker.lib.publisher.metrics')
def test_sync_flush_returns_only_transient_failures(mock_metrics):
    publisher = _make_publisher()
    publisher._client.publish.side_effect = [
        _make_future(result='msg-id-1'),
        _make_future(exc=DeadlineExceeded('retry me')),
        _make_future(exc=NotFound('drop me')),
    ]

    retry_messages = publisher._sync_flush([b'success', b'retry', b'permanent'])

    assert retry_messages == [b'retry']


@pytest.mark.asyncio
@patch('osprey.async_worker.lib.publisher.metrics')
async def test_flush_batch_requeues_transient_failures(mock_metrics):
    publisher = _make_publisher()
    publisher._client.publish.side_effect = [
        _make_future(result='msg-id-1'),
        _make_future(exc=DeadlineExceeded('retry me')),
    ]

    await publisher._flush_batch([b'success', b'retry'])

    assert publisher._queue.get_nowait() == b'retry'
    with pytest.raises(asyncio.QueueEmpty):
        publisher._queue.get_nowait()


@pytest.mark.asyncio
@patch('osprey.async_worker.lib.publisher.metrics')
async def test_flush_batch_finishes_in_flight_publish_before_cancellation(mock_metrics):
    publisher = _make_publisher()
    started = threading.Event()
    release = threading.Event()

    def sync_flush(batch):
        started.set()
        release.wait()
        return batch

    publisher._sync_flush = sync_flush
    flush_task = asyncio.create_task(publisher._flush_batch([b'retry']))
    await asyncio.to_thread(started.wait)

    flush_task.cancel()
    release.set()

    with pytest.raises(asyncio.CancelledError):
        await flush_task
    assert publisher._queue.get_nowait() == b'retry'
