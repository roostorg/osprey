from datetime import datetime, timezone
from unittest.mock import Mock, call

from osprey.engine.ast.grammar import Source
from osprey.engine.executor.execution_context import Action, ExecutionContext
from osprey.engine.executor.execution_graph import ExecutionGraph
from osprey.engine.executor.udf_execution_helpers import UDFHelpers


def test_enqueue_source_skips_sources_already_enqueued() -> None:
    entry_source = Source(path='main.sml', contents='')
    dynamic_source = Source(path='dynamic.sml', contents='')
    execution_graph = Mock(spec=ExecutionGraph)
    execution_graph.get_entry_point.return_value = entry_source
    execution_graph.get_prefolded_node_values.return_value = {}
    execution_graph.get_sorted_dependency_chain.return_value = ()
    action = Action(
        action_id=1,
        action_name='test_action',
        data={},
        timestamp=datetime(2026, 8, 3, tzinfo=timezone.utc),
    )

    context = ExecutionContext(execution_graph, action, Mock(spec=UDFHelpers))
    context.enqueue_source(dynamic_source)
    context.enqueue_source(entry_source)
    context.enqueue_source(dynamic_source)

    assert execution_graph.get_sorted_dependency_chain.call_args_list == [
        call(entry_source),
        call(dynamic_source),
    ]
