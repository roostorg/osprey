import pytest
from osprey.engine.conftest import RunValidationFunction
from osprey.engine.executor.execution_graph import ExecutionGraph, compile_execution_graph


@pytest.fixture
def compiled_execution_graph(run_validation: RunValidationFunction) -> ExecutionGraph:
    validated = run_validation(
        {
            'main.sml': 'First = 1 + 2',
            'secondary.sml': 'Second = First + 3',
        }
    )
    return compile_execution_graph(validated)
