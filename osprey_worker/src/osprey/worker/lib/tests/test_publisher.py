from unittest.mock import MagicMock, patch

import pytest
from osprey.worker.lib import publisher
from osprey.worker.lib.publisher import NullPublisher, PubSubPublisher, make_publisher


def _mock_config(pubsub_enabled: bool) -> MagicMock:
    config = MagicMock()
    config.instance.return_value.get_bool.return_value = pubsub_enabled
    return config


def test_make_publisher_returns_null_when_pubsub_disabled(caplog: pytest.LogCaptureFixture) -> None:
    with (
        patch('osprey.worker.lib.singletons.CONFIG', _mock_config(pubsub_enabled=False)),
        patch.object(publisher, 'gcp_credentials_available') as creds,
        patch.object(publisher, 'metrics') as metrics_mock,
    ):
        with caplog.at_level('WARNING', logger=publisher.logger.name):
            pub = make_publisher('proj', 'topic')
    assert isinstance(pub, NullPublisher)
    # The opt-out short-circuits before probing credentials, and is not a misconfiguration.
    assert not creds.called
    metrics_mock.increment.assert_not_called()
    assert 'PUBSUB_ENABLED is false' in caplog.text


def test_make_publisher_returns_null_and_metric_when_creds_absent(caplog: pytest.LogCaptureFixture) -> None:
    with (
        patch('osprey.worker.lib.singletons.CONFIG', _mock_config(pubsub_enabled=True)),
        patch.object(publisher, 'gcp_credentials_available', return_value=False),
        patch.object(publisher, 'metrics') as metrics_mock,
    ):
        with caplog.at_level('WARNING', logger=publisher.logger.name):
            pub = make_publisher('proj', 'topic')
    assert isinstance(pub, NullPublisher)
    assert 'credentials not detected' in caplog.text
    metrics_mock.increment.assert_called_once_with(
        'configuration.errors',
        tags=['project:proj', 'topic:topic', 'reason:gcp_credentials_missing'],
    )


def test_make_publisher_returns_pubsub_when_enabled_and_creds_present() -> None:
    with (
        patch('osprey.worker.lib.singletons.CONFIG', _mock_config(pubsub_enabled=True)),
        patch.object(publisher, 'gcp_credentials_available', return_value=True),
        patch.object(publisher, 'BatchPubsubPublisherClient') as client_cls,
    ):
        pub = make_publisher('proj', 'topic')
    assert isinstance(pub, PubSubPublisher)
    assert client_cls.called
