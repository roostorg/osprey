from types import SimpleNamespace
from typing import Iterator
from unittest.mock import MagicMock, patch

import pytest
import requests
from atproto_plugin import enrichment_udfs
from atproto_plugin.enrichment_udfs import AtprotoDisplayName, AtprotoHandle
from osprey.engine.executor.execution_context import ExpectedUdfException

PLACEHOLDER_DID = 'did:plc:aaaaaaaaaaaaaaaaaaaaaaaa'


@pytest.fixture(autouse=True)
def clear_cache() -> Iterator[None]:
    enrichment_udfs._profile_cache.clear()
    yield
    enrichment_udfs._profile_cache.clear()


def _mock_response(payload: dict) -> MagicMock:
    response = MagicMock()
    response.json.return_value = payload
    return response


def test_resolve_profile_fetches_and_caches() -> None:
    response = _mock_response({'handle': 'alice.bsky.social', 'displayName': 'Alice'})
    with patch.object(enrichment_udfs._session, 'get', return_value=response) as get:
        first = enrichment_udfs._resolve_profile(PLACEHOLDER_DID)
        second = enrichment_udfs._resolve_profile(PLACEHOLDER_DID)
    assert first == ('alice.bsky.social', 'Alice')
    assert second == first
    # Cached: the second lookup does not hit the network.
    get.assert_called_once()


def test_resolve_profile_propagates_http_error() -> None:
    response = MagicMock()
    response.raise_for_status.side_effect = requests.HTTPError('400')
    with patch.object(enrichment_udfs._session, 'get', return_value=response):
        with pytest.raises(requests.HTTPError):
            enrichment_udfs._resolve_profile(PLACEHOLDER_DID)


def test_atproto_handle_returns_handle() -> None:
    udf = AtprotoHandle.__new__(AtprotoHandle)
    with patch.object(enrichment_udfs, '_resolve_profile', return_value=('alice.bsky.social', 'Alice')):
        assert udf.execute(None, SimpleNamespace(did=PLACEHOLDER_DID)) == 'alice.bsky.social'


def test_atproto_display_name_returns_display_name() -> None:
    udf = AtprotoDisplayName.__new__(AtprotoDisplayName)
    with patch.object(enrichment_udfs, '_resolve_profile', return_value=('alice.bsky.social', 'Alice')):
        assert udf.execute(None, SimpleNamespace(did=PLACEHOLDER_DID)) == 'Alice'


def test_missing_field_raises_expected_udf_exception() -> None:
    handle_udf = AtprotoHandle.__new__(AtprotoHandle)
    name_udf = AtprotoDisplayName.__new__(AtprotoDisplayName)
    with patch.object(enrichment_udfs, '_resolve_profile', return_value=(None, None)):
        with pytest.raises(ExpectedUdfException):
            handle_udf.execute(None, SimpleNamespace(did=PLACEHOLDER_DID))
        with pytest.raises(ExpectedUdfException):
            name_udf.execute(None, SimpleNamespace(did=PLACEHOLDER_DID))


def test_transport_error_raises_expected_udf_exception() -> None:
    udf = AtprotoHandle.__new__(AtprotoHandle)
    with patch.object(enrichment_udfs, '_resolve_profile', side_effect=requests.ConnectionError()):
        with pytest.raises(ExpectedUdfException):
            udf.execute(None, SimpleNamespace(did=PLACEHOLDER_DID))
