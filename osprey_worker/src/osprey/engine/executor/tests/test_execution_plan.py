import gc
import random
from collections.abc import Iterator
from dataclasses import dataclass
from types import MappingProxyType
from unittest.mock import patch

import pytest
from osprey.engine.ast.grammar import Source, Span
from osprey.engine.conftest import RunValidationFunction
from osprey.engine.executor.dependency_chain import DependencyChain
from osprey.engine.executor.execution_graph import ExecutionGraph, compile_execution_graph
from osprey.engine.executor.execution_plan import ExecutionPlan, ExecutionPlanState, LateDependencyActivationError
from osprey.engine.executor.topological_sorter import TopologicalSorter


@dataclass(frozen=True)
class _PlanNode:
    span: Span


@dataclass(frozen=True)
class _PlanExecutor:
    node: _PlanNode


def test_full_graph_compiles_immutable_execution_plan(compiled_execution_graph: ExecutionGraph) -> None:
    plan = compiled_execution_graph.get_execution_plan()

    assert isinstance(plan, ExecutionPlan)
    assert isinstance(plan.index_by_chain_id, MappingProxyType)
    assert isinstance(plan.source_indices, MappingProxyType)
    assert len(plan.chains) == len(plan.index_by_chain_id)
    assert set(plan.source_indices) == set(compiled_execution_graph.validated_sources.sources)
    for source, indices in plan.source_indices.items():
        expected = compiled_execution_graph.get_sorted_dependency_chain(source)
        assert tuple([plan.chains[index] for index in indices]) == tuple(expected)


def test_every_planned_predecessor_has_a_stable_index(compiled_execution_graph: ExecutionGraph) -> None:
    plan = compiled_execution_graph.get_execution_plan()

    assert isinstance(plan, ExecutionPlan)
    for chain_index, chain in enumerate(plan.chains):
        expected = tuple([plan.index_by_chain_id[id(predecessor)] for predecessor in chain.dependent_on])
        assert plan.predecessors[chain_index] == expected


def test_plan_compilation_yields_through_large_phases(run_validation: RunValidationFunction) -> None:
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
        dependent_on = tuple([chains[predecessor] for predecessor in chain_predecessors])
        chains.append(
            DependencyChain(
                executor=_PlanExecutor(  # type: ignore[arg-type]
                    node=_PlanNode(span=Span(source=node_source, start_line=index + 1, start_pos=index))
                ),
                dependent_on=dependent_on,
            )
        )

    successor_lists: list[list[int]] = [[] for _ in chains]
    for successor, chain_predecessors in enumerate(predecessors):
        for predecessor in chain_predecessors:
            successor_lists[predecessor].append(successor)

    sources = tuple([Source(path=f'source-{index}.sml', contents='') for index in range(len(source_indices))])
    return (
        ExecutionPlan(
            chains=tuple(chains),
            index_by_chain_id={id(chain): index for index, chain in enumerate(chains)},
            predecessors=predecessors,
            successors=tuple([tuple(items) for items in successor_lists]),
            source_indices={source: indices for source, indices in zip(sources, source_indices)},
        ),
        sources,
    )


def _ready_indices(state: ExecutionPlanState, plan: ExecutionPlan) -> tuple[int, ...]:
    return tuple([plan.index_by_chain_id[id(chain)] for chain in state.get_ready()])


class _TupleResizeObserver:
    def __init__(self, chains: tuple[DependencyChain, ...]) -> None:
        self._chains = chains
        self._previous: DependencyChain | None = None
        self._held_tuples: list[tuple[object, ...]] = []

    def __getitem__(self, index: int) -> DependencyChain:
        if self._previous is not None:
            self._held_tuples.extend(
                referrer
                for referrer in gc.get_referrers(self._previous)
                if isinstance(referrer, tuple) and len(referrer) > len(self._chains)
            )
        chain = self._chains[index]
        self._previous = chain
        return chain


class _ObservedIndex(int):
    pass


class _TupleResizeIndexSequence:
    def __init__(self, indices: tuple[int, ...]) -> None:
        self._indices = indices
        self._held_tuples: list[tuple[object, ...]] = []

    def __iter__(self) -> Iterator[int]:
        previous: _ObservedIndex | None = None
        for index in self._indices:
            if previous is not None:
                self._held_tuples.extend(
                    referrer
                    for referrer in gc.get_referrers(previous)
                    if isinstance(referrer, tuple) and len(referrer) > len(self._indices)
                )
            previous = _ObservedIndex(index)
            yield previous


def test_get_ready_does_not_resize_an_observable_tuple() -> None:
    plan, (source,) = _manual_plan(predecessors=((), ()), source_indices=((0, 1),))
    state = ExecutionPlanState(plan)
    expected_ready = plan.chains
    observed_chains = _TupleResizeObserver(expected_ready)
    object.__setattr__(plan, 'chains', observed_chains)
    state.activate_source(source)

    ready = state.get_ready()

    assert ready == expected_ready
    assert observed_chains._held_tuples == []


def test_activate_source_reports_all_active_successors_without_tuple_resize() -> None:
    plan, (source,) = _manual_plan(predecessors=((), (), ()), source_indices=((0,),))
    observed_successors = _TupleResizeIndexSequence((1, 2))
    object.__setattr__(plan, 'successors', (observed_successors, (), ()))
    state = ExecutionPlanState(plan)
    state._active[1] = 1
    state._active[2] = 1

    with pytest.raises(LateDependencyActivationError) as exc_info:
        state.activate_source(source)

    assert 'active successor chains (1, 2)' in str(exc_info.value)
    assert observed_successors._held_tuples == []


def test_invalid_plan_reports_source_and_chain() -> None:
    plan, _ = _manual_plan(predecessors=((), (0,)), source_indices=((1,),))

    assert plan.find_unclosed_source() == (
        "source 'source-0.sml' activates chain 1 (_PlanNode at chain-1.sml:2:1) without predecessor chain 0"
    )


def test_graph_falls_back_when_plan_is_invalid(
    run_validation: RunValidationFunction, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    validated = run_validation('Value = 1')
    invalid_plan, _ = _manual_plan(predecessors=((), (0,)), source_indices=((1,),))
    monkeypatch.setattr(ExecutionPlan, 'from_graph', lambda _graph: invalid_plan)

    graph = compile_execution_graph(validated)

    assert graph.get_execution_plan() is None
    assert "Execution plan is invalid. The graph scheduler will run: source 'source-0.sml'" in caplog.text


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

    assert "source 'source-1.sml'" in str(exc_info.value)
    assert (bytes(state._active), tuple(state._remaining), tuple(state._ready)) == before


def test_plan_states_are_independent(compiled_execution_graph: ExecutionGraph) -> None:
    plan = compiled_execution_graph.get_execution_plan()
    assert isinstance(plan, ExecutionPlan)
    first = ExecutionPlanState(plan)
    second = ExecutionPlanState(plan)
    source = compiled_execution_graph.get_entry_point()

    first.activate_source(source)

    assert first.get_ready()
    assert second.get_ready() == ()
    second.activate_source(source)
    assert second.get_ready()


def test_duplicate_source_activation_is_a_noop(compiled_execution_graph: ExecutionGraph) -> None:
    plan = compiled_execution_graph.get_execution_plan()
    assert isinstance(plan, ExecutionPlan)
    state = ExecutionPlanState(plan)
    source = compiled_execution_graph.get_entry_point()

    state.activate_source(source)
    first_ready = state.get_ready()
    state.activate_source(source)

    assert first_ready
    assert state.get_ready() == ()


def test_unactivated_source_stays_unscheduled() -> None:
    plan, (entry_source, dynamic_source) = _manual_plan(predecessors=((), ()), source_indices=((0,), (1,)))
    state = ExecutionPlanState(plan)

    state.activate_source(entry_source)
    assert _ready_indices(state, plan) == (0,)
    state.done(plan.chains[0])

    assert state.get_ready() == ()
    assert dynamic_source in plan.source_indices


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


def test_source_activation_reorders_all_ready_nodes_like_legacy_prepare() -> None:
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
        predecessors = tuple(
            [tuple([index for index in range(node) if rng.random() < 0.25]) for node in range(node_count)]
        )

        def closure_for(target: int) -> tuple[int, ...]:
            closure: set[int] = set()

            def add_with_predecessors(index: int) -> None:
                for predecessor in predecessors[index]:
                    add_with_predecessors(predecessor)
                closure.add(index)

            add_with_predecessors(target)
            return tuple(sorted(closure))

        source_indices = tuple([closure_for(rng.randrange(node_count)) for _ in range(rng.randint(2, 6))])
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
                    live_predecessors = [predecessor for predecessor in predecessors[index] if predecessor in known]
                    legacy.add(index, *live_predecessors)
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
