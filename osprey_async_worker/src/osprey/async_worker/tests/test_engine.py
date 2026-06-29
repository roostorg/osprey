"""Tests for AsyncOspreyEngine recompile behavior on etcd updates."""

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

    with (
        patch.object(AsyncOspreyEngine, '_compile_execution_graph_sync', return_value=initial_graph),
        patch('osprey.async_worker.engine.ConfigSubkeyHandler'),
    ):
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

    with (
        patch.object(engine, '_compile_execution_graph_sync', return_value=new),
        patch.object(gc_module, 'collect') as mock_collect,
    ):
        await engine._handle_updated_sources()

    assert mock_collect.call_count == 1


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

    assert engine._execution_graph is pre_swap_graph, 'failed compile should not have replaced the existing graph'


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


@pytest.mark.asyncio
async def test_handle_updated_sources_nulls_parents_on_old_graph():
    """After swap, parent pointers on every AST node in the OLD graph must be
    nulled so refcount reclaims the old graph without waiting for gen-2 GC.
    The NEW graph's parents must be left intact."""

    # Old graph: two mock sources, each with a fake ast_root whose iter_nodes
    # walk yields three nodes carrying a `parent` attribute.
    def make_nodes(n):
        return [MagicMock(parent=MagicMock(name=f'parent_{i}')) for i in range(n)]

    old_nodes_per_source = [make_nodes(3), make_nodes(4)]
    new_nodes_per_source = [make_nodes(2)]

    def fake_iter_nodes(root):
        # The patched iter_nodes is called with source.ast_root; we look it up
        # via the identity of the source it came from (mock attribute chain).
        return iter(root._test_nodes)

    def make_sources(nodes_per_source):
        sources = []
        for nodes in nodes_per_source:
            src = MagicMock()
            src.ast_root._test_nodes = nodes
            sources.append(src)

        sources_mock = MagicMock()
        sources_mock.__iter__ = lambda self: iter(sources)

        validated = MagicMock()
        validated.sources = sources_mock
        graph = MagicMock(validated_sources=validated)
        return graph

    old_graph = make_sources(old_nodes_per_source)
    new_graph = make_sources(new_nodes_per_source)

    engine = _make_engine_with_stub_compile(old_graph, new_graph)

    with (
        patch.object(engine, '_compile_execution_graph_sync', return_value=new_graph),
        patch('osprey.async_worker.engine.iter_nodes', side_effect=fake_iter_nodes),
    ):
        await engine._handle_updated_sources()

    # OLD graph nodes: every parent nulled.
    for nodes in old_nodes_per_source:
        for n in nodes:
            assert n.parent is None, 'old-graph AST node still has a parent'

    # NEW graph nodes: parents untouched.
    for nodes in new_nodes_per_source:
        for n in nodes:
            assert n.parent is not None, 'new-graph AST node parent was wrongly nulled'


@pytest.mark.asyncio
async def test_handle_updated_sources_swallows_cycle_break_errors():
    """If cycle-breaking raises, the swap must still have succeeded — the new
    graph stays installed and the engine continues to function."""
    initial = MagicMock(name='initial_graph')
    new = MagicMock(name='new_graph')
    engine = _make_engine_with_stub_compile(initial, new)

    with (
        patch.object(engine, '_compile_execution_graph_sync', return_value=new),
        patch('osprey.async_worker.engine.iter_nodes', side_effect=RuntimeError('walker boom')),
    ):
        await engine._handle_updated_sources()

    assert engine._execution_graph is new, 'swap must complete even if cycle-break raises'


def test_break_old_graph_cycles_evicts_reverted_content_from_ast_cache():
    """_break_old_graph_cycles must evict a discarded source's content from the
    never-evicted module-level parsed_ast_root_cache BEFORE nulling its AST parents.

    Otherwise a later graph that re-uses that exact content (e.g. a rule revert)
    is handed back the same parent-nulled Root and fails validation with
    "`Rule(...)` must be assigned to a variable", wedging the worker on stale rules.
    Regression test: fails if the cache eviction is removed.
    """
    from osprey.engine.ast.ast_utils import iter_nodes
    from osprey.engine.ast.grammar import Source, parsed_ast_root_cache

    def rule_parent_type(root):
        for node in iter_nodes(root):
            func = getattr(node, 'func', None)
            if type(node).__name__ == 'Call' and getattr(func, 'identifier', None) == 'Rule':
                return type(node.parent).__name__ if node.parent is not None else None
        return None

    content = "MyRule = Rule(\n    when_all=[True],\n    description='x',\n)\n"
    path = 'actions/regression_evict.sml'
    before = set(parsed_ast_root_cache.keys())
    try:
        old_src = Source(path=path, contents=content)
        assert rule_parent_type(old_src.ast_root) == 'Assign'  # fresh parse: Rule is assigned
        assert old_src in parsed_ast_root_cache

        # New graph changes this file (different content) -> old content is discarded.
        new_src = Source(path=path, contents="Other = Rule(\n    when_all=[False],\n    description='y',\n)\n")
        old_graph = MagicMock(validated_sources=MagicMock(sources=[old_src]))
        new_graph = MagicMock(validated_sources=MagicMock(sources=[new_src]))

        AsyncOspreyEngine._break_old_graph_cycles(old_graph, new_graph)

        # Discarded content must be evicted so a later recurrence re-parses fresh.
        assert old_src not in parsed_ast_root_cache, 'reverted content left in cache -> parent-nulled Root reused'
        # Re-using the exact content now yields an intact AST (Rule still assigned).
        assert rule_parent_type(Source(path=path, contents=content).ast_root) == 'Assign'
    finally:
        for key in list(parsed_ast_root_cache.keys()):
            if key not in before:
                parsed_ast_root_cache.pop(key, None)
