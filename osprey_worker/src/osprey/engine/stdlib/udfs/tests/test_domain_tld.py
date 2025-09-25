from typing import Any, Callable, List

import pytest
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.udf.registry import UDFRegistry

from ....conftest import ExecuteFunction
from ..domain_tld import DomainTld

pytestmark: List[Callable[[Any], Any]] = [
    pytest.mark.use_validators([ValidateCallKwargs]),
    pytest.mark.use_udf_registry(UDFRegistry.with_udfs(DomainTld)),
]


@pytest.mark.parametrize(
    'domain,expected_result',
    [
        # Valid domain
        ('example.com', 'com'),
        ('http://www.google.co.uk', 'co.uk'),
        ('http://some.subdomain.google.co.uk', 'co.uk'),
        ('http://some.subdomain.google.us.to', 'to'),
        ('http://some.subdomain.google.us', 'us'),
        ('http://some.subdomain.google.com', 'com'),
        ('http://some.subdomain.google.tk', 'tk'),
        ('some.subdomain.notreal.co.uk', 'co.uk'),
        # Invalid domain
        ('www.google.com.fake123741', None),
        ('www.google.notreal.com.commmmmm', None),
    ],
)
def test_domain_tld(execute: ExecuteFunction, domain: str, expected_result: str) -> None:
    assert execute(f'UserDomainTld = DomainTld(domain=f"{domain}")') == {'UserDomainTld': expected_result}
