from typing import Iterator
from unittest.mock import MagicMock, patch

import pytest
from google.auth.exceptions import DefaultCredentialsError
from osprey.worker.lib.utils import gcp_credentials
from osprey.worker.lib.utils.gcp_credentials import gcp_credentials_available


@pytest.fixture(autouse=True)
def reset_cred_cache() -> Iterator[None]:
    gcp_credentials._gcp_credentials_available = None
    yield
    gcp_credentials._gcp_credentials_available = None


def test_true_when_default_resolves() -> None:
    with patch.object(gcp_credentials.google.auth, 'default', return_value=(MagicMock(), 'proj')):
        assert gcp_credentials_available() is True


def test_false_when_default_raises() -> None:
    with patch.object(gcp_credentials.google.auth, 'default', side_effect=DefaultCredentialsError()):
        assert gcp_credentials_available() is False


def test_result_is_cached() -> None:
    with patch.object(gcp_credentials.google.auth, 'default', return_value=(MagicMock(), 'proj')) as default_mock:
        gcp_credentials_available()
        gcp_credentials_available()
        gcp_credentials_available()
    assert default_mock.call_count == 1
