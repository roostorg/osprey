from unittest.mock import MagicMock

import pytest


@pytest.fixture(scope='session')
def monkeypatch_session(request):
    """Experimental (https://github.com/pytest-dev/pytest/issues/363)."""
    from _pytest.monkeypatch import MonkeyPatch

    patch = MonkeyPatch()
    yield patch
    patch.undo()


@pytest.fixture(scope='function', autouse=True)
def pubsub_client_mock(monkeypatch_session) -> MagicMock:
    # tagging as potential opensource item
    from google.cloud import pubsub_v1
    from osprey.worker.lib import publisher

    pubsub_client_mock = MagicMock()
    publisher_class_mock = MagicMock(return_value=pubsub_client_mock)

    monkeypatch_session.setattr(pubsub_v1, 'PublisherClient', publisher_class_mock)
    # PubSubPublisher degrades to noop without GCP credentials, which CI lacks;
    # force detection so the mocked client is actually exercised.
    monkeypatch_session.setattr(publisher, '_check_gcp_credentials', lambda: True)
    return pubsub_client_mock
