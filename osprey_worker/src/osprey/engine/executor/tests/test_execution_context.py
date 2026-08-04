from datetime import datetime, timezone
from unittest.mock import Mock, call, patch

import pytest
from osprey.engine.ast.grammar import Source
from osprey.engine.executor.execution_context import Action, ExecutionContext
from osprey.engine.executor.execution_graph import ExecutionGraph
from osprey.engine.executor.execution_plan import ExecutionPlan
from osprey.engine.executor.graph_specializer import SpecializedExecutionGraph
from osprey.engine.executor.udf_execution_helpers import UDFHelpers


def _action() -> Action:
    return Action(
        action_id=1,
        action_name='test_action',
        data={},
        timestamp=datetime(2026, 8, 4, tzinfo=timezone.utc),
    )


def test_context_uses_plan_for_full_graph(compiled_execution_graph: ExecutionGraph) -> None:
    context = ExecutionContext(compiled_execution_graph, _action(), Mock(spec=UDFHelpers))

    assert context._execution_plan_state is not None


def test_specialized_graph_keeps_legacy_scheduler() -> None:
    entry = Source(path='main.sml', contents='')
    graph = Mock(spec=SpecializedExecutionGraph)
    graph.get_execution_plan.return_value = None
    graph.get_entry_point.return_value = entry
    graph.get_prefolded_node_values.return_value = {}
    graph.get_sorted_dependency_chain.return_value = ()

    context = ExecutionContext(graph, _action(), Mock(spec=UDFHelpers))

    assert context._execution_plan_state is None


def test_enqueue_source_retries_after_enqueue_failure() -> None:
    entry_source = Source(path='main.sml', contents='')
    dynamic_source = Source(path='dynamic.sml', contents='')
    execution_graph = Mock(spec=ExecutionGraph)
    execution_graph.get_entry_point.return_value = entry_source
    execution_graph.get_execution_plan.return_value = None
    execution_graph.get_prefolded_node_values.return_value = {}
    execution_graph.get_sorted_dependency_chain.return_value = ()
    action = Action(
        action_id=1,
        action_name='test_action',
        data={},
        timestamp=datetime(2026, 8, 3, tzinfo=timezone.utc),
    )
    context = ExecutionContext(execution_graph, action, Mock(spec=UDFHelpers))
    execution_graph.get_sorted_dependency_chain.reset_mock()
    execution_graph.get_sorted_dependency_chain.side_effect = [RuntimeError('enqueue failed'), ()]

    with pytest.raises(RuntimeError, match='enqueue failed'):
        context.enqueue_source(dynamic_source)
    context.enqueue_source(dynamic_source)

    assert execution_graph.get_sorted_dependency_chain.call_args_list == [
        call(dynamic_source),
        call(dynamic_source),
    ]


def test_planned_enqueue_source_retries_after_activation_failure() -> None:
    entry_source = Source(path='main.sml', contents='')
    dynamic_source = Source(path='dynamic.sml', contents='')
    execution_graph = Mock(spec=ExecutionGraph)
    execution_graph.get_entry_point.return_value = entry_source
    execution_graph.get_execution_plan.return_value = Mock(spec=ExecutionPlan)
    execution_graph.get_prefolded_node_values.return_value = {}

    with patch('osprey.engine.executor.execution_context.ExecutionPlanState') as plan_state_type:
        plan_state = plan_state_type.return_value
        context = ExecutionContext(execution_graph, _action(), Mock(spec=UDFHelpers))
        plan_state.activate_source.reset_mock()
        plan_state.activate_source.side_effect = [RuntimeError('activation failed'), None]

        with pytest.raises(RuntimeError, match='activation failed'):
            context.enqueue_source(dynamic_source)
        assert dynamic_source not in context._enqueued_sources

        context.enqueue_source(dynamic_source)

    assert plan_state.activate_source.call_args_list == [call(dynamic_source), call(dynamic_source)]
    assert dynamic_source in context._enqueued_sources
