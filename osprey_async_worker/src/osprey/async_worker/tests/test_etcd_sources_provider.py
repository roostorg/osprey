"""Tests for the async etcd sources provider and input stream signaler."""

import asyncio
from unittest.mock import MagicMock, patch

import pytest

from osprey.async_worker.lib.etcd.sources_provider import (
    AsyncEtcdSourcesProvider,
    AsyncInputStreamReadySignaler,
)


# --- AsyncInputStreamReadySignaler ---


@pytest.mark.asyncio
async def test_signaler_starts_ready():
    signaler = AsyncInputStreamReadySignaler()
    assert not signaler.should_pause_input_stream()


@pytest.mark.asyncio
async def test_signaler_pause_clears_event():
    signaler = AsyncInputStreamReadySignaler()
    # Mock the jitter sleep so the test is fast
    with patch('osprey.async_worker.lib.etcd.sources_provider.asyncio.sleep', return_value=None):
        await signaler.pause_input_stream()
    assert signaler.should_pause_input_stream()


@pytest.mark.asyncio
async def test_signaler_resume_sets_event():
    signaler = AsyncInputStreamReadySignaler()
    with patch('osprey.async_worker.lib.etcd.sources_provider.asyncio.sleep', return_value=None):
        await signaler.pause_input_stream()
    signaler.resume_input_stream()
    assert not signaler.should_pause_input_stream()


@pytest.mark.asyncio
async def test_signaler_wait_blocks_when_paused():
    signaler = AsyncInputStreamReadySignaler()
    with patch('osprey.async_worker.lib.etcd.sources_provider.asyncio.sleep', return_value=None):
        await signaler.pause_input_stream()

    # wait_until_resume should block until resume is called
    resumed = False

    async def waiter():
        nonlocal resumed
        await signaler.wait_until_resume()
        resumed = True

    task = asyncio.create_task(waiter())
    await asyncio.sleep(0.01)
    assert not resumed

    signaler.resume_input_stream()
    await asyncio.sleep(0.01)
    assert resumed
    await task


@pytest.mark.asyncio
async def test_signaler_wait_returns_immediately_when_ready():
    signaler = AsyncInputStreamReadySignaler()
    # Should not block
    await asyncio.wait_for(signaler.wait_until_resume(), timeout=0.1)


# --- AsyncEtcdSourcesProvider ---


@pytest.mark.asyncio
async def test_provider_get_current_sources_default_none():
    """Before start(), sources should be None."""
    provider = AsyncEtcdSourcesProvider(etcd_key='/test/key', etcd_client=MagicMock())
    sources = provider.get_current_sources()
    assert sources is None


@pytest.mark.asyncio
async def test_provider_set_sources_watcher():
    """Watcher callback can be set."""
    provider = AsyncEtcdSourcesProvider(etcd_key='/test/key', etcd_client=MagicMock())
    callback = MagicMock()
    provider.set_sources_watcher(callback)
    assert provider._sources_watcher_callback is callback


@pytest.mark.asyncio
async def test_provider_stop_without_start():
    """Stop without start should be safe."""
    provider = AsyncEtcdSourcesProvider(etcd_key='/test/key', etcd_client=MagicMock())
    await provider.stop()  # Should not raise


# --- _handle_event callback path ---


def _make_full_sync_event(payload: dict):
    from osprey.worker.lib.etcd import FullSyncOne

    event = MagicMock(spec=FullSyncOne)
    import json as _json

    event.value = _json.dumps(payload)
    return event


@pytest.mark.asyncio
async def test_handle_event_awaits_async_callback():
    """An async (coroutine-returning) sources_watcher_callback must be awaited.

    The async engine's _handle_updated_sources is a coroutine function that
    runs compile in a thread pool. If _handle_event calls it without awaiting,
    the compile coroutine is dropped and never executes.
    """
    provider = AsyncEtcdSourcesProvider(etcd_key='/test/key', etcd_client=MagicMock())
    awaited = asyncio.Event()

    async def async_callback() -> None:
        awaited.set()

    provider.set_sources_watcher(async_callback)

    with patch('osprey.async_worker.lib.etcd.sources_provider.asyncio.sleep', return_value=None):
        await provider._handle_event(_make_full_sync_event({'main.sml': '# noop'}))

    assert awaited.is_set(), 'async callback was not awaited'


@pytest.mark.asyncio
async def test_handle_event_calls_sync_callback():
    """A plain (non-coroutine) callback continues to work for back-compat."""
    provider = AsyncEtcdSourcesProvider(etcd_key='/test/key', etcd_client=MagicMock())
    callback = MagicMock()
    provider.set_sources_watcher(callback)

    with patch('osprey.async_worker.lib.etcd.sources_provider.asyncio.sleep', return_value=None):
        await provider._handle_event(_make_full_sync_event({'main.sml': '# noop'}))

    callback.assert_called_once()


@pytest.mark.asyncio
async def test_handle_event_skips_callback_on_unchanged_hash():
    """Hash-equality short-circuit: if the new sources match current, no callback."""
    provider = AsyncEtcdSourcesProvider(etcd_key='/test/key', etcd_client=MagicMock())
    payload = {'main.sml': '# noop'}

    # Seed _current_sources with the same content the next event will deliver.
    from osprey.engine.ast.sources import Sources

    provider._current_sources = Sources.from_dict(payload)

    callback = MagicMock()
    provider.set_sources_watcher(callback)

    await provider._handle_event(_make_full_sync_event(payload))

    callback.assert_not_called()
