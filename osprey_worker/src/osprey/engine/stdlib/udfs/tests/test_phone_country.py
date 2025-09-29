from typing import Any, Callable, List

import pytest
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.conftest import ExecuteFunction
from osprey.engine.stdlib.udfs.phone_country import PhoneCountry
from osprey.engine.udf.registry import UDFRegistry

pytestmark: List[Callable[[Any], Any]] = [
    pytest.mark.use_validators([ValidateCallKwargs]),
    pytest.mark.use_udf_registry(UDFRegistry.with_udfs(PhoneCountry)),
]


@pytest.mark.parametrize(
    'phone_number,country',
    [('+15555555555', 'US'), ('+65555555555', 'AU'), ('+5545555555555', 'BR')],
)
def test_accepts_valid_country_code(execute: ExecuteFunction, phone_number: str, country: str) -> None:
    result = execute(f'PhoneCountryStr = PhoneCountry(phone_number="{phone_number}")')
    assert result == {'PhoneCountryStr': country}


@pytest.mark.parametrize(
    'phone_number',
    ['not a number', '135555555555', '1395555555555'],
)
def test_invalid_country_code_failure(execute: ExecuteFunction, phone_number: str) -> None:
    result = execute(f'PhoneCountryStr = PhoneCountry(phone_number="{phone_number}")')
    assert result == {'PhoneCountryStr': None}
