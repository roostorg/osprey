from typing import Any, Callable, List

import pytest
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.conftest import ExecuteFunction
from osprey.engine.stdlib.udfs.get_action_id import GetActionId
from osprey.engine.udf.registry import UDFRegistry

pytestmark: List[Callable[[Any], Any]] = [
    pytest.mark.use_validators([ValidateCallKwargs]),
    pytest.mark.use_udf_registry(UDFRegistry.with_udfs(GetActionId)),
]


def test_execute(execute: ExecuteFunction) -> None:
    data = execute('ActionId = GetActionId()', data={}, action_id=42)

    assert data == {'ActionId': 42}
