from typing import Dict, Optional

import pytest
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.udf.registry import UDFRegistry

from ....conftest import ExecuteFunction
from ..email_domain import EmailDomain, EmailSubdomain
from ..json_data import JsonData

pytestmark = [
    pytest.mark.use_validators([ValidateCallKwargs]),
    pytest.mark.use_udf_registry(UDFRegistry.with_udfs(JsonData, EmailDomain)),
    pytest.mark.use_udf_registry(UDFRegistry.with_udfs(JsonData, EmailSubdomain)),
]

sources = """
    UserEmail: str = JsonData(path='$.email', required=False)
    UserEmailDomain = EmailDomain(email=UserEmail)
    UserEmailSubdomain = EmailSubdomain(email=UserEmail)
"""


def test_accepts_valid_json_data_email(execute: ExecuteFunction) -> None:
    result = execute(sources, data={'email': 'a@example.com'})

    assert result == {
        'UserEmail': 'a@example.com',
        'UserEmailDomain': 'example.com',
        'UserEmailSubdomain': 'example.com',
    }


def test_accepts_valid_json_data_email_subdomain(execute: ExecuteFunction) -> None:
    result = execute(sources, data={'email': 'a@test.example.com'})

    assert result == {
        'UserEmail': 'a@test.example.com',
        'UserEmailDomain': 'example.com',
        'UserEmailSubdomain': 'test.example.com',
    }


def test_accepts_json_data_email_empty(execute: ExecuteFunction) -> None:
    result = execute(sources, data={})

    assert result == {'UserEmail': None, 'UserEmailDomain': None, 'UserEmailSubdomain': None}


@pytest.mark.parametrize(
    'data,expected_result',
    [
        ({'email': 'a'}, None),
        ({'email': 'b@'}, None),
        ({'email': 'b@test@example.org'}, 'example.org'),
        ({'email': 'd@eXaMpLe.gov'}, 'example.gov'),
        ({'email': 'e@me.example.co.uk'}, 'example.co.uk'),
        ({'email': '@example'}, 'example'),
        ({'email': 'blah@test.notarealtld'}, 'test.notarealtld'),
    ],
)
def test_invalid_email_addresses(
    execute: ExecuteFunction, data: Dict[str, object], expected_result: Optional[str]
) -> None:
    result = execute(sources, data=data)

    assert result['UserEmailDomain'] == expected_result


@pytest.mark.parametrize(
    'data,expected_result',
    [
        ({'email': 'a'}, None),
        ({'email': 'b@'}, None),
        ({'email': 'b@test@example.org'}, 'example.org'),
        ({'email': 'd@eXaMpLe.gov'}, 'example.gov'),
        ({'email': 'e@me.example.co.uk'}, 'example.co.uk'),
        ({'email': '@example'}, 'example'),
        ({'email': 'blah@test.notarealtld'}, 'test.notarealtld'),
    ],
)
def test_invalid_email_subdomain_addresses(
    execute: ExecuteFunction, data: Dict[str, object], expected_result: Optional[str]
) -> None:
    result = execute(sources, data=data)

    assert result['UserEmailSubdomain'] == expected_result
