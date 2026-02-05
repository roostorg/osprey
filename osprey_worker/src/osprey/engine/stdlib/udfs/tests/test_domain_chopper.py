from collections.abc import Callable
from typing import Any

import pytest
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.conftest import ExecuteFunction
from osprey.engine.stdlib.udfs.domain_chopper import DomainChopper
from osprey.engine.udf.registry import UDFRegistry

pytestmark: list[Callable[[Any], Any]] = [
    pytest.mark.use_validators([ValidateCallKwargs]),
    pytest.mark.use_udf_registry(UDFRegistry.with_udfs(DomainChopper)),
]


@pytest.mark.parametrize(
    'urls, expected_result',
    [
        (['google.com'], ['google.com']),
        (['subdomain.example.com'], ['example.com']),
        (['www.example.co.uk'], ['example.co.uk']),
        (['ftp.anotherexample.org'], ['anotherexample.org']),
        (
            ['subdomain.example.com', 'google.com'],
            ['example.com', 'google.com'],
        ),
    ],
)
def test_domain_chopper_fld(execute: ExecuteFunction, urls: list, expected_result: list) -> None:
    assert execute(f'Domain = DomainChopper(urls={urls}, fld=True )') == {'Domain': expected_result}
