from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.conftest import ExecuteFunction
from osprey.engine.stdlib.udfs.mx_lookup import MXLookup
from osprey.engine.udf.registry import UDFRegistry

pytestmark: list[Callable[[Any], Any]] = [
    pytest.mark.use_validators([ValidateCallKwargs]),
    pytest.mark.use_udf_registry(UDFRegistry.with_udfs(MXLookup)),
]


def fake_resolve(domain: str, record_type: str, raise_on_no_answer: bool) -> list[Any]:
    if record_type == 'MX':
        return [MagicMock()]
    if record_type == 'A':
        m = MagicMock()
        m.to_text.return_value = '123.456.789'
        return [m]
    return []


def test_golden_path_mx_lookup(execute: ExecuteFunction) -> None:
    with patch(MXLookup.__module__ + '.resolver.resolve', side_effect=fake_resolve):
        result = execute('IP = MXLookup(domain="example.com")')
        assert result == {'IP': '123.456.789'}
