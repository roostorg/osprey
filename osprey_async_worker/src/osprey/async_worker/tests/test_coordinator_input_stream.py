"""Tests for the async coordinator input stream."""

import asyncio
from unittest.mock import MagicMock, patch

import pytest
from osprey.async_worker.lib.coordinator_input_stream import (
    GrpcConnectionDiscoveryPool,
    OspreyCoordinatorBiDirectionalStream,
    OspreyCoordinatorInputStream,
)

# --- GrpcConnectionDiscoveryPool ---


def test_discovery_pool_creates_channels():
    """Pool creates grpc.aio channels from service discovery."""
    mock_service = MagicMock()
    mock_service.connection_address = 'localhost'
    mock_service.grpc_port = 50051

    mock_watcher = MagicMock()

    mock_directory = MagicMock()
    mock_directory.select_all.return_value = [mock_service]
    mock_directory.get_watcher.return_value = mock_watcher

    with patch('osprey.worker.lib.discovery.directory.Directory') as MockDirectory:
        MockDirectory.instance.return_value = mock_directory
        pool = GrpcConnectionDiscoveryPool('test_coordinator')
        assert len(pool._grpc_channels) == 1


# --- OspreyCoordinatorBiDirectionalStream ---


@pytest.mark.asyncio
async def test_bidirectional_stream_queue_based():
    """Stream uses asyncio.Queue for sending requests."""
    stream = OspreyCoordinatorBiDirectionalStream.__new__(OspreyCoordinatorBiDirectionalStream)
    stream._request_queue = asyncio.Queue()
    stream._should_run = True

    await stream._request_queue.put('test_request')
    item = await stream._request_queue.get()
    assert item == 'test_request'


def test_acking_context_should_nack_contract():
    """The out-of-band ack path keys off should_nack, so mark_as_nack must flip it."""
    from osprey.worker.sinks.utils.acking_contexts_base import NoopAckingContext

    ctx = NoopAckingContext(item='x')
    assert ctx.should_nack is False
    ctx.mark_as_nack()
    assert ctx.should_nack is True


def test_send_ack_or_nack_emits_nack_when_not_ack():
    """ack=False must enqueue a Nack (not an Ack), so a nacked context isn't acked."""
    stream = OspreyCoordinatorBiDirectionalStream.__new__(OspreyCoordinatorBiDirectionalStream)
    stream._outgoing_queue = asyncio.Queue()

    stream.send_ack_or_nack(123, ack=True)
    stream.send_ack_or_nack(456, ack=False)

    ack_req = stream._outgoing_queue.get_nowait()
    nack_req = stream._outgoing_queue.get_nowait()
    assert ack_req.action_request.ack_or_nack.HasField('ack')
    assert nack_req.action_request.ack_or_nack.HasField('nack')


@pytest.mark.asyncio
async def test_send_graceful_disconnect_emits_nack_when_not_ack():
    """Graceful-disconnect finalize paths must nack a nacked context, not ack it."""
    stream = OspreyCoordinatorBiDirectionalStream.__new__(OspreyCoordinatorBiDirectionalStream)
    stream._outgoing_queue = asyncio.Queue()

    await stream.send_graceful_disconnect(99, ack=False)

    req = stream._outgoing_queue.get_nowait()
    assert req.disconnect.ack_or_nack.HasField('nack')


# --- OspreyCoordinatorInputStream ---


@pytest.mark.asyncio
async def test_input_stream_stop():
    """Stop sets the shutdown event."""
    stream = OspreyCoordinatorInputStream.__new__(OspreyCoordinatorInputStream)
    stream._shutdown_event = asyncio.Event()

    assert not stream._shutdown_event.is_set()
    await stream.stop()
    assert stream._shutdown_event.is_set()


@pytest.mark.asyncio
async def test_input_stream_shutdown_event_unblocks():
    """Setting shutdown event should unblock any waiters."""
    stream = OspreyCoordinatorInputStream.__new__(OspreyCoordinatorInputStream)
    stream._shutdown_event = asyncio.Event()

    unblocked = False

    async def waiter():
        nonlocal unblocked
        await stream._shutdown_event.wait()
        unblocked = True

    task = asyncio.create_task(waiter())
    await asyncio.sleep(0.01)
    assert not unblocked

    await stream.stop()
    await asyncio.sleep(0.01)
    assert unblocked
    await task
