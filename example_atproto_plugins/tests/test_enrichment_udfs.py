import time
from types import SimpleNamespace
from typing import Iterator
from unittest.mock import MagicMock, patch

import pytest
import requests
from atproto_plugin import enrichment_udfs
from atproto_plugin.enrichment_udfs import AtprotoDisplayName, AtprotoHandle
from osprey.engine.executor.execution_context import ExpectedUdfException

PLACEHOLDER_DID = 'did:plc:aaaaaaaaaaaaaaaaaaaaaaaa'
SAMPLE_PROFILE = {'did': PLACEHOLDER_DID, 'handle': 'alice.bsky.social', 'displayName': 'Alice'}


@pytest.fixture(autouse=True)
def clear_cache() -> Iterator[None]:
    enrichment_udfs._profile_cache.clear()
    enrichment_udfs._inflight.clear()
    yield
    enrichment_udfs._profile_cache.clear()
    enrichment_udfs._inflight.clear()


def _mock_response(payload: object) -> MagicMock:
    response = MagicMock()
    response.json.return_value = payload
    return response


def test_fetch_profile_fetches_and_caches() -> None:
    with patch.object(enrichment_udfs._session, 'get', return_value=_mock_response(SAMPLE_PROFILE)) as get:
        first = enrichment_udfs._fetch_profile(PLACEHOLDER_DID)
        second = enrichment_udfs._fetch_profile(PLACEHOLDER_DID)
    assert first == SAMPLE_PROFILE
    assert second == first
    # Cached: the second lookup does not hit the network.
    get.assert_called_once()


def test_fetch_profile_propagates_http_error() -> None:
    response = MagicMock()
    response.raise_for_status.side_effect = requests.HTTPError('400')
    with patch.object(enrichment_udfs._session, 'get', return_value=response):
        with pytest.raises(requests.HTTPError):
            enrichment_udfs._fetch_profile(PLACEHOLDER_DID)
    # A failed fetch leaves nothing behind, so the next lookup retries.
    assert PLACEHOLDER_DID not in enrichment_udfs._inflight


def test_fetch_rides_on_in_progress_request() -> None:
    # Simulate another greenlet already fetching this DID: the entry is present in
    # _inflight with its result set. A second caller must ride on it, not re-request.
    inflight = enrichment_udfs._InflightFetch()
    inflight.profile = SAMPLE_PROFILE
    inflight.done.set()
    enrichment_udfs._inflight[PLACEHOLDER_DID] = inflight

    with patch.object(enrichment_udfs._session, 'get') as get:
        result = enrichment_udfs._fetch_profile(PLACEHOLDER_DID)

    assert result == SAMPLE_PROFILE
    get.assert_not_called()


def test_fetch_reraises_in_progress_error() -> None:
    inflight = enrichment_udfs._InflightFetch()
    inflight.error = requests.ConnectionError()
    inflight.done.set()
    enrichment_udfs._inflight[PLACEHOLDER_DID] = inflight

    with patch.object(enrichment_udfs._session, 'get') as get:
        with pytest.raises(requests.ConnectionError):
            enrichment_udfs._fetch_profile(PLACEHOLDER_DID)
    get.assert_not_called()


def test_expired_entry_triggers_refetch() -> None:
    with patch.object(enrichment_udfs._session, 'get', return_value=_mock_response(SAMPLE_PROFILE)) as get:
        enrichment_udfs._fetch_profile(PLACEHOLDER_DID)
        assert get.call_count == 1

        # Jump past the TTL so the cached entry is considered stale and refetched.
        stale = time.monotonic() + enrichment_udfs._CACHE_TTL_SECONDS + 1
        with patch.object(enrichment_udfs.time, 'monotonic', return_value=stale):
            enrichment_udfs._fetch_profile(PLACEHOLDER_DID)
        assert get.call_count == 2


def test_atproto_handle_returns_handle() -> None:
    udf = AtprotoHandle.__new__(AtprotoHandle)
    with patch.object(enrichment_udfs, '_fetch_profile', return_value=SAMPLE_PROFILE):
        assert udf.execute(None, SimpleNamespace(did=PLACEHOLDER_DID)) == 'alice.bsky.social'


def test_atproto_display_name_returns_display_name() -> None:
    udf = AtprotoDisplayName.__new__(AtprotoDisplayName)
    with patch.object(enrichment_udfs, '_fetch_profile', return_value=SAMPLE_PROFILE):
        assert udf.execute(None, SimpleNamespace(did=PLACEHOLDER_DID)) == 'Alice'


def test_missing_field_raises_expected_udf_exception() -> None:
    handle_udf = AtprotoHandle.__new__(AtprotoHandle)
    name_udf = AtprotoDisplayName.__new__(AtprotoDisplayName)
    with patch.object(enrichment_udfs, '_fetch_profile', return_value={'did': PLACEHOLDER_DID}):
        with pytest.raises(ExpectedUdfException):
            handle_udf.execute(None, SimpleNamespace(did=PLACEHOLDER_DID))
        with pytest.raises(ExpectedUdfException):
            name_udf.execute(None, SimpleNamespace(did=PLACEHOLDER_DID))


def test_transport_error_raises_expected_udf_exception() -> None:
    udf = AtprotoHandle.__new__(AtprotoHandle)
    with patch.object(enrichment_udfs, '_fetch_profile', side_effect=requests.ConnectionError()):
        with pytest.raises(ExpectedUdfException):
            udf.execute(None, SimpleNamespace(did=PLACEHOLDER_DID))
