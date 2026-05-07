"""Tests for AsyncOspreyEngine recompile behavior on etcd updates."""

import asyncio
import threading
from unittest.mock import MagicMock, patch

import pytest

from osprey.async_worker.engine import AsyncOspreyEngine


def _make_engine_with_stub_compile(initial_graph, recompile_graph):
    """Build an AsyncOspreyEngine without exercising the real compile path.

    Patches _compile_execution_graph_sync so __init__ uses initial_graph and
    later recompiles return recompile_graph. Returns the constructed engine.
    """
    sources_provider = MagicMock()
    sources_provider.get_current_sources.return_value = MagicMock(hash=MagicMock(return_value='abc'))

    udf_registry = MagicMock()

    with patch.object(AsyncOspreyEngine, '_compile_execution_graph_sync', return_value=initial_graph), \
         patch('osprey.async_worker.engine.ConfigSubkeyHandler'):
        engine = AsyncOspreyEngine(
            sources_provider=sources_provider,
            udf_registry=udf_registry,
        )

    # Register what subsequent compiles should return.
    engine._stub_recompile_graph = recompile_graph
    return engine


@pytest.mark.asyncio
async def test_handle_updated_sources_runs_compile_off_event_loop():
    """The compile must run in self._thread_pool, not on the event loop thread.

    When the loop is blocked by the compile, in-flight gRPC tasks cannot drain
    and their pinned response buffers stack up alongside the compile-transient
    memory. Routing through self._thread_pool keeps the loop responsive.
    """
    initial = MagicMock(name='initial_graph')
    new = MagicMock(name='new_graph')
    engine = _make_engine_with_stub_compile(initial, new)

    loop_thread = threading.get_ident()
    compile_thread = {}

    def fake_sync_compile():
        compile_thread['tid'] = threading.get_ident()
        return new

    with patch.object(engine, '_compile_execution_graph_sync', side_effect=fake_sync_compile):
        await engine._handle_updated_sources()

    assert 'tid' in compile_thread, 'compile did not run'
    assert compile_thread['tid'] != loop_thread, (
        'compile ran on the event-loop thread; it must run in the thread pool '
        'so in-flight tasks can drain during compile'
    )
    assert engine._execution_graph is new


@pytest.mark.asyncio
async def test_handle_updated_sources_does_not_force_gc():
    """We deliberately do NOT call gc.collect() after swap.

    Forcing gen-2 collection promotes every surviving object to gen 2, which
    makes subsequent automatic collections during action processing more
    expensive. We let CPython's reference counting reclaim the old graph
    naturally — same as the gevent engine.
    """
    import gc as gc_module
    initial = MagicMock(name='initial_graph')
    new = MagicMock(name='new_graph')
    engine = _make_engine_with_stub_compile(initial, new)

    with patch.object(engine, '_compile_execution_graph_sync', return_value=new), \
         patch.object(gc_module, 'collect') as mock_collect:
        await engine._handle_updated_sources()

    assert mock_collect.call_count == 0


@pytest.mark.asyncio
async def test_handle_updated_sources_keeps_old_graph_on_compile_failure():
    """If the new compile raises, _execution_graph must still point at the old
    graph. Otherwise in-flight actions would crash on a missing graph."""
    initial = MagicMock(name='initial_graph')
    new = MagicMock(name='new_graph')
    engine = _make_engine_with_stub_compile(initial, new)
    pre_swap_graph = engine._execution_graph

    with patch.object(engine, '_compile_execution_graph_sync', side_effect=RuntimeError('boom')):
        await engine._handle_updated_sources()

    assert engine._execution_graph is pre_swap_graph, (
        'failed compile should not have replaced the existing graph'
    )


@pytest.mark.asyncio
async def test_handle_updated_sources_dispatches_config_after_swap():
    """After a successful recompile, the config subkey handler must be notified
    with the validated_sources of the new graph."""
    initial = MagicMock(name='initial_graph')
    new = MagicMock(name='new_graph')
    engine = _make_engine_with_stub_compile(initial, new)
    engine._config_subkey_handler = MagicMock()

    with patch.object(engine, '_compile_execution_graph_sync', return_value=new):
        await engine._handle_updated_sources()

    engine._config_subkey_handler.dispatch_config.assert_called_once_with(new.validated_sources)
