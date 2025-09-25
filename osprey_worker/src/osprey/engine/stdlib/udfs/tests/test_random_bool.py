from typing import Any, Callable, List

import pytest
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.udf.registry import UDFRegistry

from ....conftest import CheckFailureFunction, ExecuteFunction, RunValidationFunction
from ..random_bool import RandomBool

pytestmark: List[Callable[[Any], Any]] = [
    pytest.mark.use_validators([ValidateCallKwargs, UniqueStoredNames]),
    pytest.mark.use_udf_registry(UDFRegistry.with_udfs(RandomBool)),
]


def test_random_bool(execute: ExecuteFunction) -> None:
    """
    If this test flakes even one single time then patch out random.random and play the lottery
    """
    assert execute('r = RandomBool(percentage=0.00000000000001)') == {'r': False}
    assert execute('r = RandomBool(percentage=0.99999999999999)') == {'r': True}


def test_random_bool_validation(run_validation: RunValidationFunction, check_failure: CheckFailureFunction) -> None:
    with check_failure():
        run_validation('r = RandomBool(percentage=1.1)')


def test_random_bool_validation_1(run_validation: RunValidationFunction, check_failure: CheckFailureFunction) -> None:
    with check_failure():
        run_validation('r = RandomBool(percentage=1.0)')


def test_random_bool_validation_0(run_validation: RunValidationFunction, check_failure: CheckFailureFunction) -> None:
    with check_failure():
        run_validation('r = RandomBool(percentage=0.0)')
