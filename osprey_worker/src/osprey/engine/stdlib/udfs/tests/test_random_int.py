from collections.abc import Callable
from typing import Any

import pytest
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.conftest import CheckFailureFunction, ExecuteFunction, RunValidationFunction
from osprey.engine.stdlib.udfs.random_int import RandomInt
from osprey.engine.udf.registry import UDFRegistry

pytestmark: list[Callable[[Any], Any]] = [
    pytest.mark.use_validators([ValidateCallKwargs, UniqueStoredNames]),
    pytest.mark.use_udf_registry(UDFRegistry.with_udfs(RandomInt)),
]


def test_random_int(execute: ExecuteFunction) -> None:
    assert execute('r: bool = RandomInt(start=48,end=143) >=48') == {'r': True}
    assert execute('r: bool = RandomInt(start=48,end=143) <= 143') == {'r': True}
    assert execute('r: bool = RandomInt(start=48,end=143) < 48') == {'r': False}
    assert execute('r: bool = RandomInt(start=48,end=143) > 143') == {'r': False}


def test_random_int_validation(run_validation: RunValidationFunction, check_failure: CheckFailureFunction) -> None:
    with check_failure():
        run_validation('r = RandomInt(start=10,end=0)')


def test_random_int_validation_0(run_validation: RunValidationFunction, check_failure: CheckFailureFunction) -> None:
    with check_failure():
        run_validation('r = RandomInt(start=0,end=0)')
