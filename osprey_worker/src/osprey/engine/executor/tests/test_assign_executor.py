from unittest.mock import Mock

import pytest
from osprey.engine.ast.ast_utils import filter_nodes
from osprey.engine.ast.grammar import Assign, Source
from osprey.engine.executor.execution_context import ExecutionContext, NodeFailurePropagationException
from osprey.engine.executor.node_executor.assign_executor import AssignExecutor
from result import Err, Ok


def _parse_assign(contents: str) -> Assign:
    source = Source(path='test.sml', contents=contents)
    return next(iter(filter_nodes(source.ast_root, Assign)))


def test_assign_resolves_its_value_once() -> None:
    assign = _parse_assign('Result = 1\n')
    execution_context = Mock(spec=ExecutionContext)
    execution_context.resolved_result.return_value = Ok(1)
    executor = AssignExecutor(assign, Mock())

    assert executor.execute(execution_context) == 1
    execution_context.resolved_result.assert_called_once_with(assign.value)


def test_assign_extracts_none_and_propagates_a_failed_value() -> None:
    assign = _parse_assign('Result = Function()\n')
    execution_context = Mock(spec=ExecutionContext)
    execution_context.resolved_result.return_value = Err(None)
    executor = AssignExecutor(assign, Mock())

    with pytest.raises(NodeFailurePropagationException):
        executor.execute(execution_context)

    execution_context.set_output_value.assert_called_once_with('Result', None)
