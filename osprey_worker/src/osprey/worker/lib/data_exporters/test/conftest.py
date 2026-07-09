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
def pubsub_client_mock(monkeypatch_session, monkeypatch) -> MagicMock:
    # tagging as potential opensource item
    from google.cloud import pubsub_v1
    from osprey.worker.lib import publisher

    pubsub_client_mock = MagicMock()
    publisher_class_mock = MagicMock(return_value=pubsub_client_mock)

    monkeypatch_session.setattr(pubsub_v1, 'PublisherClient', publisher_class_mock)
    # PubSubPublisher degrades to noop without GCP credentials, which CI lacks;
    # force detection so the mocked client is actually exercised. Function-scoped
    # monkeypatch so it reverts after each test instead of leaking into later
    # modules (e.g. test_publisher.py, whose disabled-publisher tests would then
    # build a real client and hang on stop()).
    monkeypatch.setattr(publisher, 'gcp_credentials_available', lambda: True)
    return pubsub_client_mock
