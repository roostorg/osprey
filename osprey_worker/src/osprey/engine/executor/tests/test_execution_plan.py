import random
from unittest.mock import patch

import pytest
from osprey.engine.ast.grammar import Source
from osprey.engine.conftest import RunValidationFunction
from osprey.engine.executor.dependency_chain import DependencyChain
from osprey.engine.executor.execution_graph import ExecutionGraph, compile_execution_graph
from osprey.engine.executor.execution_plan import (
    ExecutionPlan,
    ExecutionPlanState,
    LateDependencyActivationError,
)
from osprey.engine.executor.topological_sorter import TopologicalSorter


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
        chains.append(
            DependencyChain(
                executor=object(),  # type: ignore[arg-type]
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

    with pytest.raises(LateDependencyActivationError):
        state.activate_source(predecessor_source)

    assert (bytes(state._active), tuple(state._remaining), tuple(state._ready)) == before


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
