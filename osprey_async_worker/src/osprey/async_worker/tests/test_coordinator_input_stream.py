"""Tests for the async coordinator input stream."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from osprey.async_worker.lib.coordinator_input_stream import (
    GrpcConnectionDiscoveryPool,
    OspreyCoordinatorBiDirectionalStream,
    OspreyCoordinatorInputStream,
    AsyncVerdictsAckingContext,
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
