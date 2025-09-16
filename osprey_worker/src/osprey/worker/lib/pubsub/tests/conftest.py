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

    pubsub_client_mock = MagicMock()
    publisher_class_mock = MagicMock(return_value=pubsub_client_mock)

    monkeypatch_session.setattr(pubsub_v1, 'PublisherClient', publisher_class_mock)
    return pubsub_client_mock
