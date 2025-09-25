from typing import Any, Callable, List

import pytest
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.udf.registry import UDFRegistry

from ....conftest import ExecuteFunction
from ..extract_cookie import ExtractCookie
from ..json_data import JsonData

pytestmark: List[Callable[[Any], Any]] = [
    pytest.mark.use_validators([ValidateCallKwargs]),
    pytest.mark.use_udf_registry(UDFRegistry.with_udfs(JsonData, ExtractCookie)),
]

sources = """
    HttpCookieHeader: str = JsonData(path='$.http.headers.Cookie', required=False)
    HttpFooCookieValue = ExtractCookie(header=HttpCookieHeader, key='foo')
    HttpBarCookieValue = ExtractCookie(header=HttpCookieHeader, key='bar')
"""


def test_parses_cookies(execute: ExecuteFunction) -> None:
    header = 'foo=hello; bar=goodbye'
    result = execute(sources, data={'http': {'headers': {'Cookie': header}}})
    assert result == {'HttpCookieHeader': header, 'HttpFooCookieValue': 'hello', 'HttpBarCookieValue': 'goodbye'}


def test_parses_cookies_handles_not_exists(execute: ExecuteFunction) -> None:
    header = 'foo=hello'
    result = execute(sources, data={'http': {'headers': {'Cookie': header}}})
    assert result == {'HttpCookieHeader': header, 'HttpFooCookieValue': 'hello', 'HttpBarCookieValue': None}


def test_parses_invalid_cookies(execute: ExecuteFunction) -> None:
    header = 'not a cookie, but you know, we should handle this!'
    result = execute(sources, data={'http': {'headers': {'Cookie': header}}})
    assert result == {'HttpCookieHeader': header, 'HttpFooCookieValue': None, 'HttpBarCookieValue': None}
