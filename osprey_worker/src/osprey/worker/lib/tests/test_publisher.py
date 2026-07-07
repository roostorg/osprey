from unittest.mock import MagicMock, patch

import pytest
from osprey.worker.lib import publisher
from osprey.worker.lib.publisher import PubSubPublisher


def test_pubsub_publisher_constructs_client_when_creds_present() -> None:
    with (
        patch.object(publisher, 'gcp_credentials_available', return_value=True),
        patch.object(publisher, 'BatchPubsubPublisherClient') as client_cls,
    ):
        pub = PubSubPublisher('proj', 'topic')
    assert pub._enabled is True
    assert client_cls.called


def test_pubsub_publisher_noops_when_creds_absent(caplog: pytest.LogCaptureFixture) -> None:
    with (
        patch.object(publisher, 'gcp_credentials_available', return_value=False),
        patch.object(publisher, 'BatchPubsubPublisherClient') as client_cls,
    ):
        with caplog.at_level('WARNING', logger=publisher.logger.name):
            pub = PubSubPublisher('proj', 'topic')
    assert pub._enabled is False
    assert not client_cls.called
    assert 'noop mode' in caplog.text
    assert 'project=proj' in caplog.text
    assert 'topic=topic' in caplog.text


def test_pubsub_publisher_disabled_via_env(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv('DISABLE_GCP_PUBSUB', 'true')
    with (
        patch.object(publisher, 'gcp_credentials_available') as cred_check,
        patch.object(publisher, 'BatchPubsubPublisherClient') as client_cls,
    ):
        with caplog.at_level('WARNING', logger=publisher.logger.name):
            pub = PubSubPublisher('proj', 'topic')
    assert pub._enabled is False
    assert not client_cls.called
    # The opt-out short-circuits before probing credentials.
    assert not cred_check.called
    assert 'DISABLE_GCP_PUBSUB' in caplog.text


def test_publish_short_circuits_silently_when_disabled() -> None:
    with (
        patch.object(publisher, 'gcp_credentials_available', return_value=False),
        patch.object(publisher, 'metrics') as metrics_mock,
    ):
        pub = PubSubPublisher('proj', 'topic')
        pub.publish(MagicMock())
    metrics_mock.increment.assert_not_called()


def test_stop_short_circuits_when_disabled() -> None:
    with patch.object(publisher, 'gcp_credentials_available', return_value=False):
        pub = PubSubPublisher('proj', 'topic')
        pub.stop()
    assert not hasattr(pub, '_publisher')
