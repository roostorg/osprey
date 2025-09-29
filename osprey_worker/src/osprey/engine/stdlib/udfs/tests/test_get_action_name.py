from typing import Any, Callable, List

import pytest
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.conftest import ExecuteFunction
from osprey.engine.stdlib.udfs.get_action_name import GetActionName
from osprey.engine.udf.registry import UDFRegistry

pytestmark: List[Callable[[Any], Any]] = [
    pytest.mark.use_validators([ValidateCallKwargs]),
    pytest.mark.use_udf_registry(UDFRegistry.with_udfs(GetActionName)),
]


def test_execute(execute: ExecuteFunction) -> None:
    data = execute('ActionName = GetActionName()', data={})

    assert data == {'ActionName': 'test'}
