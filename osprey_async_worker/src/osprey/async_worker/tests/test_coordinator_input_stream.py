"""Tests for the async coordinator input stream."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from osprey.async_worker.lib.coordinator_input_stream import (
    GrpcConnectionDiscoveryPool,
    OspreyCoordinatorBiDirectionalStream,
    OspreyCoordinatorInputStream,
)
from osprey.rpc.osprey_coordinator.bidirectional_stream.v1.service_pb2 import (
    ActionRequest,
    ClientDetails,
    Request,
)
from osprey.worker.lib.discovery.service import Service

# --- GrpcConnectionDiscoveryPool ---


@pytest.mark.asyncio
async def test_discovery_pool_creates_channels():
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
        try:
            assert len(pool._grpc_channels) == 1
        finally:
            await pool.close()


# --- OspreyCoordinatorBiDirectionalStream ---


@pytest.mark.asyncio
async def test_bidirectional_stream_sends_until_stop_signal():
    """Outgoing requests are yielded in order until the stop sentinel."""
    service = Service(name='test_coordinator', address='localhost', port=50051)
    request = Request(action_request=ActionRequest(initial=ClientDetails(id='test-client')))

    with patch('osprey.async_worker.lib.coordinator_input_stream.OspreyCoordinatorServiceStub'):
        stream = OspreyCoordinatorBiDirectionalStream('test-client', MagicMock(), service)

    await stream._send(request)
    await stream._enqueue_stop_signal()

    assert [outgoing async for outgoing in stream._outgoing_iterator()] == [request]


# --- OspreyCoordinatorInputStream ---


@pytest.mark.asyncio
async def test_input_stream_stop():
    """Stop sets the shutdown event."""
    stream = OspreyCoordinatorInputStream.__new__(OspreyCoordinatorInputStream)
    stream._shutdown_event = asyncio.Event()
    stream._channel_pool = MagicMock()
    stream._channel_pool.close = AsyncMock()

    assert not stream._shutdown_event.is_set()
    await stream.stop()
    assert stream._shutdown_event.is_set()
    stream._channel_pool.close.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_input_stream_shutdown_event_unblocks():
    """Setting shutdown event should unblock any waiters."""
    stream = OspreyCoordinatorInputStream.__new__(OspreyCoordinatorInputStream)
    stream._shutdown_event = asyncio.Event()
    stream._channel_pool = MagicMock()
    stream._channel_pool.close = AsyncMock()

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
    stream._channel_pool.close.assert_awaited_once_with()
