from collections.abc import Callable
from typing import Any

import pytest
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.conftest import ExecuteFunction
from osprey.engine.stdlib.udfs.email_local_part import EmailLocalPart
from osprey.engine.udf.registry import UDFRegistry

pytestmark: list[Callable[[Any], Any]] = [
    pytest.mark.use_validators([ValidateCallKwargs]),
    pytest.mark.use_udf_registry(UDFRegistry.with_udfs(EmailLocalPart)),
]


@pytest.mark.parametrize(
    'email, expected_result',
    [
        ('example@example.com', 'example'),
        ('example_with_underscores', None),
        ('example@', 'example'),
        ('@example', ''),
        ('test@email@example.com', 'test@email'),
        ('tEsT@eXaMpLe.org', 'tEsT'),
    ],
)
def test_email_local_part(execute: ExecuteFunction, email: str, expected_result: str | None) -> None:
    result = execute(f'UserEmailLocalPart = EmailLocalPart(email="{email}")')
    assert result == {'UserEmailLocalPart': expected_result}
