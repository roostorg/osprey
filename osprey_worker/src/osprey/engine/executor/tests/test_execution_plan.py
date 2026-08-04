from unittest.mock import patch

from osprey.engine.conftest import RunValidationFunction
from osprey.engine.executor.execution_graph import ExecutionGraph, compile_execution_graph
from osprey.engine.executor.execution_plan import ExecutionPlan


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
