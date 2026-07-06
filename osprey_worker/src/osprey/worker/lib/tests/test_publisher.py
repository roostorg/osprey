from typing import Iterator
from unittest.mock import MagicMock, patch

import pytest
from google.auth.exceptions import DefaultCredentialsError
from osprey.worker.lib import publisher
from osprey.worker.lib.publisher import PubSubPublisher, _check_gcp_credentials


@pytest.fixture(autouse=True)
def reset_cred_cache() -> Iterator[None]:
    publisher._gcp_credentials_available = None
    yield
    publisher._gcp_credentials_available = None


def test_check_gcp_credentials_true_when_default_resolves() -> None:
    with patch.object(publisher.google.auth, 'default', return_value=(MagicMock(), 'proj')):
        assert _check_gcp_credentials() is True


def test_check_gcp_credentials_false_when_default_raises() -> None:
    with patch.object(publisher.google.auth, 'default', side_effect=DefaultCredentialsError()):
        assert _check_gcp_credentials() is False


def test_check_gcp_credentials_is_cached() -> None:
    with patch.object(publisher.google.auth, 'default', return_value=(MagicMock(), 'proj')) as default_mock:
        _check_gcp_credentials()
        _check_gcp_credentials()
        _check_gcp_credentials()
    assert default_mock.call_count == 1


def test_pubsub_publisher_constructs_client_when_creds_present() -> None:
    with (
        patch.object(publisher.google.auth, 'default', return_value=(MagicMock(), 'proj')),
        patch.object(publisher, 'BatchPubsubPublisherClient') as client_cls,
    ):
        pub = PubSubPublisher('proj', 'topic')
    assert pub._enabled is True
    assert client_cls.called


def test_pubsub_publisher_noops_when_creds_absent(caplog: pytest.LogCaptureFixture) -> None:
    with (
        patch.object(publisher.google.auth, 'default', side_effect=DefaultCredentialsError()),
        patch.object(publisher, 'BatchPubsubPublisherClient') as client_cls,
    ):
        with caplog.at_level('WARNING', logger=publisher.logger.name):
            pub = PubSubPublisher('proj', 'topic')
    assert pub._enabled is False
    assert not client_cls.called
    assert 'noop mode' in caplog.text
    assert 'project=proj' in caplog.text
    assert 'topic=topic' in caplog.text


def test_publish_short_circuits_and_emits_noop_metric_when_disabled() -> None:
    with (
        patch.object(publisher.google.auth, 'default', side_effect=DefaultCredentialsError()),
        patch.object(publisher, 'metrics') as metrics_mock,
    ):
        pub = PubSubPublisher('proj', 'topic')
        pub.publish(MagicMock())
    metrics_mock.increment.assert_called_once_with(
        'PubSubPublisher.publisher.noop',
        tags=['project:proj', 'topic:projects/proj/topics/topic'],
    )


def test_stop_short_circuits_when_disabled() -> None:
    with patch.object(publisher.google.auth, 'default', side_effect=DefaultCredentialsError()):
        pub = PubSubPublisher('proj', 'topic')
        pub.stop()
    assert not hasattr(pub, '_publisher')
