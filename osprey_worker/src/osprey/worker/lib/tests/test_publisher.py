from unittest.mock import MagicMock, patch

import pytest
from google.auth.exceptions import DefaultCredentialsError
from osprey.worker.lib import publisher
from osprey.worker.lib.publisher import NullPublisher, PubSubPublisher, make_publisher


def _mock_config(pubsub_enabled: bool) -> MagicMock:
    config = MagicMock()
    config.instance.return_value.get_bool.return_value = pubsub_enabled
    return config


def test_make_publisher_returns_null_when_pubsub_not_enabled() -> None:
    config = _mock_config(pubsub_enabled=False)
    with (
        patch('osprey.worker.lib.singletons.CONFIG', config),
        patch.object(publisher, 'BatchPubsubPublisherClient') as client_cls,
        patch.object(publisher, 'metrics') as metrics_mock,
    ):
        pub = make_publisher('proj', 'topic')
    assert isinstance(pub, NullPublisher)
    # On by default; explicitly disabling is not a misconfiguration, so no client and no metric.
    config.instance.return_value.get_bool.assert_called_once_with('OSPREY_PUBSUB_ENABLED', True)
    assert not client_cls.called
    metrics_mock.increment.assert_not_called()


def test_make_publisher_returns_pubsub_when_enabled_and_creds_present() -> None:
    with (
        patch('osprey.worker.lib.singletons.CONFIG', _mock_config(pubsub_enabled=True)),
        patch.object(publisher, 'BatchPubsubPublisherClient') as client_cls,
    ):
        pub = make_publisher('proj', 'topic')
    assert isinstance(pub, PubSubPublisher)
    assert client_cls.called


def test_make_publisher_falls_back_to_null_and_metric_when_creds_absent(caplog: pytest.LogCaptureFixture) -> None:
    with (
        patch('osprey.worker.lib.singletons.CONFIG', _mock_config(pubsub_enabled=True)),
        patch.object(publisher, 'BatchPubsubPublisherClient', side_effect=DefaultCredentialsError()),
        patch.object(publisher, 'metrics') as metrics_mock,
    ):
        with caplog.at_level('WARNING', logger=publisher.logger.name):
            pub = make_publisher('proj', 'topic')
    assert isinstance(pub, NullPublisher)
    assert 'credentials errored' in caplog.text
    metrics_mock.increment.assert_called_once_with(
        'configuration.errors',
        tags=['project:proj', 'topic:topic', 'reason:gcp_credentials_missing'],
    )
