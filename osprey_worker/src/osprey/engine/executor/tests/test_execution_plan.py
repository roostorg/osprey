import gc
import random
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterator, List, Sequence, Tuple
from unittest.mock import patch

import pytest
from osprey.engine.ast.grammar import Source, Span
from osprey.engine.conftest import RunValidationFunction
from osprey.engine.executor.dependency_chain import DependencyChain
from osprey.engine.executor.execution_context import Action, ExecutionContext
from osprey.engine.executor.execution_graph import ExecutionGraph, compile_execution_graph
from osprey.engine.executor.execution_plan import (
    ExecutionPlan,
    ExecutionPlanState,
    LateDependencyActivationError,
)
from osprey.engine.executor.graph_specializer import SpecializedExecutionGraph
from osprey.engine.executor.topological_sorter import TopologicalSorter
from osprey.engine.executor.udf_execution_helpers import UDFHelpers
from osprey.engine.schema.schema_loader import ActionSchema
from result import Err


@dataclass(frozen=True)
class _PlanNode:
    span: Span


@dataclass(frozen=True)
class _PlanExecutor:
    node: _PlanNode


def test_full_graph_compiles_execution_plan(compiled_execution_graph: ExecutionGraph) -> None:
    plan = compiled_execution_graph.get_execution_plan()

    assert plan is not None
    assert len(plan.chains) == len(plan.index_by_chain_id)
    assert set(plan.source_indices) == set(compiled_execution_graph.validated_sources.sources)
    for source, indices in plan.source_indices.items():
        expected = compiled_execution_graph.get_sorted_dependency_chain(source)
        assert tuple(plan.chains[index] for index in indices) == tuple(expected)


def test_every_planned_predecessor_has_a_stable_index(compiled_execution_graph: ExecutionGraph) -> None:
    plan = compiled_execution_graph.get_execution_plan()

    assert plan is not None
    for chain_index, chain in enumerate(plan.chains):
        assert plan.predecessors[chain_index] == tuple(
            plan.index_by_chain_id[id(predecessor)] for predecessor in chain.dependent_on
        )


def test_plan_compilation_yields_through_every_large_phase(run_validation: RunValidationFunction) -> None:
    validated = run_validation(
        {
            'main.sml': 'First = 1 + 2',
            'secondary.sml': 'Second = First + 3',
        }
    )

    with patch('osprey.engine.executor.execution_plan.maybe_periodic_yield') as periodic_yield:
        graph = compile_execution_graph(validated)

    plan = graph.get_execution_plan()
    assert isinstance(plan, ExecutionPlan)
    chain_references = sum(len(indices) for indices in plan.source_indices.values())
    assert periodic_yield.call_count >= 4 * len(plan.chains) + 2 * chain_references


def _manual_plan(
    predecessors: tuple[tuple[int, ...], ...], source_indices: tuple[tuple[int, ...], ...]
) -> tuple[ExecutionPlan, tuple[Source, ...]]:
    chains: list[DependencyChain] = []
    for chain_predecessors in predecessors:
        index = len(chains)
        node_source = Source(path=f'chain-{index}.sml', contents='')
        chains.append(
            DependencyChain(
                executor=_PlanExecutor(  # type: ignore[arg-type]
                    node=_PlanNode(span=Span(source=node_source, start_line=index + 1, start_pos=index))
                ),
                dependent_on=tuple(chains[index] for index in chain_predecessors),
            )
        )

    successors: list[list[int]] = [[] for _ in chains]
    for successor, chain_predecessors in enumerate(predecessors):
        for predecessor in chain_predecessors:
            successors[predecessor].append(successor)

    sources = tuple(Source(path=f'source-{index}.sml', contents='') for index in range(len(source_indices)))
    return (
        ExecutionPlan(
            chains=tuple(chains),
            index_by_chain_id={id(chain): index for index, chain in enumerate(chains)},
            predecessors=predecessors,
            successors=tuple(tuple(items) for items in successors),
            source_indices={source: indices for source, indices in zip(sources, source_indices)},
        ),
        sources,
    )


def _ready_indices(state: ExecutionPlanState, plan: ExecutionPlan) -> tuple[int, ...]:
    return tuple(plan.index_by_chain_id[id(chain)] for chain in state.get_ready())


class _TupleResizeObserver:
    def __init__(self, chains: tuple[DependencyChain, ...]) -> None:
        self._chains = chains
        self._previous: DependencyChain | None = None
        self._held_tuples: list[tuple[object, ...]] = []

    def __getitem__(self, index: int) -> DependencyChain:
        if self._previous is not None:
            # Retaining the in-progress tuple simulates a sampler observing it before CPython shrinks it.
            self._held_tuples.extend(
                referrer
                for referrer in gc.get_referrers(self._previous)
                if isinstance(referrer, tuple) and len(referrer) > len(self._chains)
            )
        chain = self._chains[index]
        self._previous = chain
        return chain


def test_get_ready_does_not_resize_an_observable_tuple() -> None:
    plan, (source,) = _manual_plan(predecessors=((), ()), source_indices=((0, 1),))
    state = ExecutionPlanState(plan)
    expected_ready = plan.chains
    observed_chains = _TupleResizeObserver(expected_ready)
    object.__setattr__(plan, 'chains', observed_chains)
    state.activate_source(source)

    ready = state.get_ready()

    assert ready == expected_ready


def test_plan_states_are_independent(compiled_execution_graph: ExecutionGraph) -> None:
    plan = compiled_execution_graph.get_execution_plan()
    assert plan is not None
    first = ExecutionPlanState(plan)
    second = ExecutionPlanState(plan)
    source = compiled_execution_graph.get_entry_point()

    first.activate_source(source)

    assert first.get_ready()
    assert not second.get_ready()
    second.activate_source(source)
    assert second.get_ready()


def test_duplicate_source_activation_is_a_noop(compiled_execution_graph: ExecutionGraph) -> None:
    plan = compiled_execution_graph.get_execution_plan()
    assert plan is not None
    state = ExecutionPlanState(plan)
    source = compiled_execution_graph.get_entry_point()

    state.activate_source(source)
    first_ready = state.get_ready()
    state.activate_source(source)

    assert state.get_ready() == ()
    assert first_ready


def test_activation_failure_does_not_mutate_state() -> None:
    plan, (dependent_source, predecessor_source) = _manual_plan(
        predecessors=((), (0,)),
        source_indices=((1,), (0,)),
    )
    state = ExecutionPlanState(plan)
    state.activate_source(dependent_source)
    before = (bytes(state._active), tuple(state._remaining), tuple(state._ready))

    with pytest.raises(LateDependencyActivationError) as exc_info:
        state.activate_source(predecessor_source)

    message = str(exc_info.value)
    assert "source 'source-1.sml'" in message
    assert 'chain 0 (_PlanNode at chain-0.sml:1:0)' in message
    assert 'active successor chain(s) (1,)' in message
    assert (bytes(state._active), tuple(state._remaining), tuple(state._ready)) == before


def test_find_unclosed_source_flags_exactly_what_activation_cannot_schedule() -> None:
    """The build-time check must reject the plan shape that raises at activation time (the one
    `test_activation_failure_does_not_mutate_state` drives) and accept its closed counterpart."""
    unclosed, _ = _manual_plan(predecessors=((), (0,)), source_indices=((1,), (0,)))
    closed, _ = _manual_plan(predecessors=((), (0,)), source_indices=((0, 1), (0,)))

    message = unclosed.find_unclosed_source()
    assert message is not None
    assert "source 'source-0.sml' activates chain 1" in message
    assert '_PlanNode at chain-1.sml:2:1' in message
    assert 'without its predecessor chain 0' in message
    assert closed.find_unclosed_source() is None


def test_successors_become_ready_in_dynamic_activation_order() -> None:
    plan, (second_successor_source, first_successor_source) = _manual_plan(
        predecessors=((), (0,), (0,)),
        source_indices=((0, 2), (0, 1)),
    )
    state = ExecutionPlanState(plan)
    state.activate_source(second_successor_source)
    state.activate_source(first_successor_source)
    assert _ready_indices(state, plan) == (0,)

    state.done(plan.chains[0])

    assert _ready_indices(state, plan) == (2, 1)


def test_source_activation_reorders_all_ready_nodes_like_prepare() -> None:
    plan, (dependent_source, existing_ready_source, new_ready_source) = _manual_plan(
        predecessors=((), (0,), (), ()),
        source_indices=((0, 1), (2,), (3,)),
    )
    state = ExecutionPlanState(plan)
    state.activate_source(dependent_source)
    assert _ready_indices(state, plan) == (0,)
    state.activate_source(existing_ready_source)
    state.done(plan.chains[0])
    assert tuple(state._ready) == (2, 1)

    state.activate_source(new_ready_source)

    assert _ready_indices(state, plan) == (1, 2, 3)


def test_scheduler_matches_legacy_sorter_for_randomized_valid_activations() -> None:
    rng = random.Random(0)
    transitions = 0

    while transitions < 10_000:
        node_count = rng.randint(2, 9)
        predecessors = tuple(tuple(index for index in range(node) if rng.random() < 0.25) for node in range(node_count))

        def closure_for(target: int) -> tuple[int, ...]:
            closure: set[int] = set()

            def add_with_predecessors(index: int) -> None:
                for predecessor in predecessors[index]:
                    add_with_predecessors(predecessor)
                closure.add(index)

            add_with_predecessors(target)
            return tuple(sorted(closure))

        source_indices = tuple(closure_for(rng.randrange(node_count)) for _ in range(rng.randint(2, 6)))
        source_indices += (tuple(range(node_count)),)
        plan, sources = _manual_plan(predecessors, source_indices)
        state = ExecutionPlanState(plan)
        legacy = TopologicalSorter()
        legacy_added: set[int] = set()
        sources_left = list(sources)
        outstanding: list[int] = []

        while True:
            if sources_left and (not outstanding or rng.random() < 0.45):
                source_position = rng.randrange(len(sources_left))
                source = sources_left.pop(source_position)
                indices = plan.source_indices[source]
                known = legacy_added | set(indices)
                for index in indices:
                    if legacy.already_added(index):
                        continue
                    legacy.add(index, *(predecessor for predecessor in predecessors[index] if predecessor in known))
                    legacy_added.add(index)
                legacy.prepare()
                state.activate_source(source)
                transitions += 1
            elif outstanding and rng.random() < 0.65:
                position = rng.randrange(len(outstanding))
                index = outstanding.pop(position)
                legacy.done(index)
                state.done(plan.chains[index])
                transitions += 1
            else:
                expected = legacy.get_ready()
                actual = _ready_indices(state, plan)
                assert actual == expected
                outstanding.extend(expected)
                transitions += 1
                if not expected and not sources_left and not outstanding:
                    break


# ---------------------------------------------------------------------------
# Differential over a SPECIALIZED graph, whose per-source lists are FILTERED
# ---------------------------------------------------------------------------

_FILTERED_ACTION = 'filtered_action'

_FILTERED_SOURCES = {
    # `Dup = A + A` gives a chain a DUPLICATE predecessor edge, which both schedulers must count
    # (and decrement) twice. The cross-source name reads below need no Import because the
    # `run_validation` fixture registers no validators; only the compiled chain graph matters here,
    # and a globally stored name resolves to the same chain object either way.
    'main.sml': """
        A = 1 + 2
        B = A + 3
        C = B + A
        D = C + 4
        E = D + B
        Dup = A + A
    """,
    # Both are activated dynamically, in randomized order (what runtime Requires do), and every
    # statement reads a name defined in main.sml or the other dynamic source. Once chains are
    # filtered out, these are the surviving chains whose `dependent_on` points at chains the plan
    # never indexes.
    'dynamic_one.sml': """
        F = A + D
        G = F + C
        H = G + E
        DupF = F + F
    """,
    'dynamic_two.sml': """
        K = G + Dup
        L = K + E
        M = L + DupF
    """,
}


def _filtered_specialization(
    full_graph: ExecutionGraph, pruned: 'frozenset[int]', folded: 'frozenset[int]'
) -> SpecializedExecutionGraph:
    """A real SpecializedExecutionGraph with an explicit filtered set.

    Choosing the pruned/folded keys directly (rather than deriving them from a schema) is what
    lets this differential randomize the filtered set. Which chains a schema *should* filter is
    covered by test_graph_specializer.py's golden gates.
    """
    return SpecializedExecutionGraph(
        full_graph=full_graph,
        pruned_keys=pruned,
        schema=ActionSchema(
            action=_FILTERED_ACTION,
            provides_groups=frozenset(),
            absent_groups=frozenset(),
            provides_field_types={},
            optional_for={},
        ),
        fold_values={key: Err(None) for key in folded},
    )


@contextmanager
def _forced_legacy_scheduler(graph: ExecutionGraph) -> Iterator[None]:
    """Run ``graph`` on the legacy TopologicalSorter — what every specialized graph used before
    per-schema plans existed."""
    plan = graph._execution_plan
    graph._execution_plan = None
    try:
        yield
    finally:
        graph._execution_plan = plan


def _lockstep(
    graph: SpecializedExecutionGraph, dynamic_sources: Sequence[Source], rng: random.Random
) -> Tuple[int, List[DependencyChain]]:
    """Drive the plan and the legacy sorter over the SAME specialized graph in lockstep, with a
    randomized interleaving of dynamic activations, completions and get_ready polls.

    Returns (transitions, chains handed out in order). Asserts ready-order equality at every poll.
    """
    action = Action(action_id=1, action_name=_FILTERED_ACTION, data={}, timestamp=datetime(2020, 1, 1))
    planned = ExecutionContext(graph, action, UDFHelpers())
    with _forced_legacy_scheduler(graph):
        legacy = ExecutionContext(graph, action, UDFHelpers())
    assert planned._execution_plan_state is not None
    assert legacy._execution_plan_state is None and legacy._dependency_dag is not None

    handed_out: List[DependencyChain] = []
    outstanding: List[DependencyChain] = []
    pending = list(dynamic_sources)
    rng.shuffle(pending)
    transitions = 0
    while True:
        transitions += 1
        roll = rng.random()
        if pending and roll < 0.2:
            source = pending.pop()
            planned.enqueue_source(source)
            legacy.enqueue_source(source)
        elif outstanding and roll < 0.7:
            chain = outstanding.pop(rng.randrange(len(outstanding)))
            planned.set_resolved_value(chain, Err(None))
            legacy.set_resolved_value(chain, Err(None))
        else:
            ready = tuple(planned.get_ready_to_execute())
            assert ready == tuple(legacy.get_ready_to_execute()), 'plan and legacy sorter disagreed on ready order'
            outstanding.extend(ready)
            handed_out.extend(ready)
            if not ready and not pending and not outstanding:
                return transitions, handed_out


def test_specialized_scheduler_matches_legacy_sorter_for_randomized_filtered_graphs(
    run_validation: RunValidationFunction,
) -> None:
    """A filtered (pruned + constant-folded) graph must schedule exactly as the legacy
    TopologicalSorter scheduled it, including for sources activated dynamically mid-execution, in
    randomized order, whose surviving chains depend on filtered ones.

    A filtered chain is absent from EVERY source's list, so it has no plan index, is never handed
    out and is never marked done. ``ExecutionPlan.from_graph`` therefore drops the edge, which is
    what ``ExecutionContext._enqueue_source_legacy``'s ``live_pred_ids`` filter does. Get this
    wrong in the other direction and the countdown for a surviving chain never reaches zero, so
    the strongest oracle here is that every surviving chain is still handed out exactly once.
    """
    validated = run_validation(_FILTERED_SOURCES)
    full_graph = compile_execution_graph(validated)
    entry_source = full_graph.get_entry_point()
    dynamic_sources = [source for source in validated.sources if source.path.startswith('dynamic_')]
    assert len(dynamic_sources) == 2 and entry_source not in dynamic_sources
    activated_sources = [entry_source, *dynamic_sources]
    unique_chains: Dict[int, DependencyChain] = {}
    for source in activated_sources:
        for chain in full_graph.get_sorted_dependency_chain(source):
            unique_chains[id(chain)] = chain
    all_chains = tuple(unique_chains.values())
    assert any(len(set(map(id, chain.dependent_on))) < len(chain.dependent_on) for chain in all_chains), (
        'fixture must contain a duplicate predecessor edge'
    )

    rng = random.Random(20260805)
    transitions = 0
    trials = 0
    saw_dropped_edge = False
    while transitions < 10_000:
        keys = [id(chain.executor.node) for chain in all_chains]
        rng.shuffle(keys)
        split = rng.randrange(len(keys) // 2)
        pruned = frozenset(keys[:split])
        folded = frozenset(keys[split : split + rng.randrange(len(keys) // 2)])
        filtered = pruned | folded
        graph = _filtered_specialization(full_graph, pruned, folded)

        plan = graph.get_execution_plan()
        assert plan is not None, 'a filtered graph must still get a plan'
        assert plan.find_unclosed_source() is None, 'filtering must preserve per-source closure'
        for chain in plan.chains:
            index = plan.index_by_chain_id[id(chain)]
            # Recomputed from the filtered key set, not from the implementation. Duplicates are
            # kept: legacy counts a repeated predecessor once per occurrence.
            live = [predecessor for predecessor in chain.dependent_on if id(predecessor.executor.node) not in filtered]
            assert plan.predecessors[index] == tuple(plan.index_by_chain_id[id(p)] for p in live)
            saw_dropped_edge = saw_dropped_edge or len(live) < len(chain.dependent_on)

        step, handed_out = _lockstep(graph, dynamic_sources, rng)
        transitions += step
        trials += 1

        schedulable = {id(chain) for source in activated_sources for chain in graph.get_sorted_dependency_chain(source)}
        assert {id(chain) for chain in handed_out} == schedulable, 'a surviving chain was never scheduled'
        assert len(handed_out) == len(schedulable), 'a chain was handed out more than once'

    assert saw_dropped_edge, 'no trial produced a surviving chain with a filtered predecessor'
    assert trials > 1
