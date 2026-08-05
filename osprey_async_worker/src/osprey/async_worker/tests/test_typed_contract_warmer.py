"""Tests for the background typed-contract specialization warmer.

The specializer itself is stubbed at the module seam (``specialize_one_action`` /
``build_specialization_index``, mirroring how test_typed_contract_dispatch.py stubs
``dispatch``): what is under test is the queue discipline, the reload identity guard and
the failure negative-cache, not the specialization.
"""

import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from typing import List, Optional
from unittest.mock import MagicMock

import pytest
from osprey.async_worker import typed_contract_warmer as warmer_mod
from osprey.async_worker.typed_contract_warmer import TypedContractWarmer

_ACTIONS = ['action_a', 'action_b', 'action_c']


class _FakeGraph:
    """Minimal stand-in for an ExecutionGraph: only the schemas map is reached."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.validated_sources = SimpleNamespace(
            sources=SimpleNamespace(schemas=lambda: {f'schemas/{a}.json': '{}' for a in _ACTIONS})
        )

    def __repr__(self) -> str:
        return f'<_FakeGraph {self.name}>'


@pytest.fixture
def pool():
    with ThreadPoolExecutor(max_workers=1) as executor:
        yield executor


@pytest.fixture(autouse=True)
def _no_warm_interval(monkeypatch):
    """Zero the inter-action sleep so the drain loop is bounded by the fake specializer."""
    monkeypatch.setenv('OSPREY_TYPED_CONTRACT_WARM_INTERVAL_MS', '0')
    monkeypatch.setattr(warmer_mod, 'build_specialization_index', lambda graph: object())


def _make_warmer(pool, graph_holder, registered, *, on_drained=None) -> TypedContractWarmer:
    return TypedContractWarmer(
        graph_provider=lambda: graph_holder['graph'],
        register_filter_provider=lambda: frozenset({'*'}),
        action_names_provider=lambda: _ACTIONS,
        register=lambda name, spec: registered.append((name, spec)),
        thread_pool=pool,
        on_drained=on_drained,
        yield_during_specialize=False,
    )


async def test_seed_warms_every_filter_matching_action(pool, monkeypatch):
    graph_holder = {'graph': _FakeGraph('g1')}
    registered: List = []
    monkeypatch.setattr(warmer_mod, 'specialize_one_action', lambda graph, action, **kw: f'spec:{action}')

    warmer = _make_warmer(pool, graph_holder, registered)
    warmer.seed()
    assert warmer.pending_count == len(_ACTIONS), 'seed must queue every allowlisted action'
    await warmer._task

    assert registered == [(a, f'spec:{a}') for a in _ACTIONS]
    assert warmer.pending_count == 0


async def test_traffic_miss_moves_action_to_front_of_queue(pool, monkeypatch):
    """An action that takes traffic before it is warm must be warmed FIRST — the whole
    point of seeding the rest of the corpus behind it."""
    graph_holder = {'graph': _FakeGraph('g1')}
    order: List[str] = []

    def fake_specialize(graph, action, **kwargs):
        order.append(action)
        return f'spec:{action}'

    monkeypatch.setattr(warmer_mod, 'specialize_one_action', fake_specialize)

    warmer = _make_warmer(pool, graph_holder, [])
    warmer.seed()
    warmer.note_miss('action_c')  # last in the seeded order
    await warmer._task

    assert order[0] == 'action_c', f'traffic miss did not jump the queue: {order}'
    assert sorted(order) == sorted(_ACTIONS), 'promotion must not drop or duplicate work'


async def test_repeated_traffic_misses_do_not_requeue_the_same_action(pool, monkeypatch):
    """note_miss is on the dispatch hot path and fires on every miss until the action is
    warm; it must be idempotent, not grow the queue."""
    graph_holder = {'graph': _FakeGraph('g1')}
    monkeypatch.setattr(warmer_mod, 'specialize_one_action', lambda graph, action, **kw: f'spec:{action}')

    warmer = _make_warmer(pool, graph_holder, [])
    warmer.seed()
    for _ in range(5):
        warmer.note_miss('action_c')
    assert warmer.pending_count == len(_ACTIONS)


async def test_reload_mid_warmup_never_publishes_the_old_graphs_specialization(pool, monkeypatch):
    """A specialization in flight when the graph is swapped carries folds derived from the
    RETIRED corpus. It must be dropped at publish, and the action re-queued."""
    graph_holder = {'graph': _FakeGraph('old')}
    registered: List = []
    started = threading.Event()
    release = threading.Event()
    seen_graphs: List[str] = []

    def fake_specialize(graph, action, **kwargs):
        seen_graphs.append(graph.name)
        if graph.name == 'old':
            started.set()
            release.wait(timeout=5)
        return f'spec:{action}@{graph.name}'

    monkeypatch.setattr(warmer_mod, 'specialize_one_action', fake_specialize)

    warmer = _make_warmer(pool, graph_holder, registered)
    warmer.seed()
    assert await asyncio.to_thread(started.wait, 5)

    # The reload: swap the graph and retire the warmer's state, exactly as the engine does.
    graph_holder['graph'] = _FakeGraph('new')
    warmer.reset()
    warmer.seed()
    release.set()
    await warmer._task

    assert all(spec.endswith('@new') for _, spec in registered), (
        f'a specialization built against the retired graph was published: {registered}'
    )
    assert sorted(name for name, _ in registered) == sorted(_ACTIONS), (
        'the action whose in-flight specialization was dropped must be re-warmed'
    )


async def test_specialize_failure_is_negative_cached_for_the_generation(pool, monkeypatch):
    """A failing action must be attempted exactly once per graph generation — never retried
    from the dispatch hot path — and must never publish anything."""
    graph_holder = {'graph': _FakeGraph('g1')}
    registered: List = []
    attempts: List[str] = []

    def boom(graph, action, **kwargs):
        attempts.append(action)
        raise RuntimeError('specialize exploded')

    monkeypatch.setattr(warmer_mod, 'specialize_one_action', boom)

    warmer = _make_warmer(pool, graph_holder, registered)
    warmer.seed()
    await warmer._task

    assert attempts == _ACTIONS
    assert registered == [], 'a failed specialization must not be published'

    # Dispatch keeps missing on these actions; the negative cache must absorb it.
    for _ in range(3):
        for action in _ACTIONS:
            warmer.note_miss(action)
    assert warmer.pending_count == 0, 'negative-cached actions were re-queued'
    if warmer._task is not None and not warmer._task.done():
        await warmer._task
    assert attempts == _ACTIONS, 'a failed action was retried against the same graph'

    # A new graph generation clears the negative cache — the failure may have been the
    # corpus, not the schema.
    graph_holder['graph'] = _FakeGraph('g2')
    warmer.reset()
    warmer.seed()
    await warmer._task
    assert attempts == _ACTIONS * 2


async def test_action_without_a_schema_is_settled_not_retried(pool, monkeypatch):
    graph_holder = {'graph': _FakeGraph('g1')}
    registered: List = []
    calls: List[str] = []

    def no_schema(graph, action, **kwargs) -> Optional[str]:
        calls.append(action)
        return None

    monkeypatch.setattr(warmer_mod, 'specialize_one_action', no_schema)

    warmer = _make_warmer(pool, graph_holder, registered)
    warmer.seed()
    await warmer._task

    assert registered == []
    for action in _ACTIONS:
        warmer.note_miss(action)
    assert warmer.pending_count == 0, 'schema-less allowlisted actions must be negative-cached'
    assert calls == _ACTIONS


async def test_drain_hook_is_debounced_to_once_per_generation(pool, monkeypatch):
    """The re-freeze hook exists to pull warm-up allocations into the permanent GC
    generation. gc.freeze() is not free, so a second drain in the same generation must not
    fire it again."""
    graph_holder = {'graph': _FakeGraph('g1')}
    hook = MagicMock()
    monkeypatch.setattr(warmer_mod, 'specialize_one_action', lambda graph, action, **kw: f'spec:{action}')

    warmer = _make_warmer(pool, graph_holder, [], on_drained=hook)
    warmer.seed()
    await warmer._task
    assert hook.call_count == 1

    # A traffic miss for an action outside the seeded set starts a second drain that DOES
    # specialize something — the case the debounce has to absorb.
    warmer.note_miss('action_d')
    await warmer._task
    assert hook.call_count == 1, 'the re-freeze must be debounced to once per generation'

    graph_holder['graph'] = _FakeGraph('g2')
    warmer.reset()
    warmer.seed()
    await warmer._task
    assert hook.call_count == 2, 'a new generation re-arms the re-freeze'


async def test_seed_is_a_noop_when_the_allowlist_is_empty(pool):
    """The typed-contract gates default OFF; the warmer must not touch the corpus at all
    until an action is allowlisted."""

    def boom():
        raise AssertionError('action names must not be enumerated when the allowlist is empty')

    warmer = TypedContractWarmer(
        graph_provider=lambda: _FakeGraph('g1'),
        register_filter_provider=frozenset,
        action_names_provider=boom,
        register=lambda name, spec: None,
        thread_pool=pool,
    )
    warmer.seed()
    assert warmer.pending_count == 0
    assert warmer._task is None


def test_seed_without_a_running_loop_queues_work_for_later(pool, monkeypatch):
    """Engine __init__ is synchronous and pre-loop. Seeding there must still fill the queue;
    the task starts on the first reload or dispatch miss."""
    monkeypatch.setattr(warmer_mod, 'specialize_one_action', lambda graph, action, **kw: f'spec:{action}')
    graph_holder = {'graph': _FakeGraph('g1')}
    registered: List = []
    warmer = _make_warmer(pool, graph_holder, registered)

    warmer.seed()
    assert warmer._task is None, 'no loop at boot; nothing should have been scheduled'
    assert warmer.pending_count == len(_ACTIONS)

    async def drive():
        warmer.ensure_running()
        await warmer._task

    asyncio.run(drive())
    assert sorted(name for name, _ in registered) == sorted(_ACTIONS)
