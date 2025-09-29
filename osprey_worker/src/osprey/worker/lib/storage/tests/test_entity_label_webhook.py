from __future__ import absolute_import

from typing import Iterator

import pytest
from osprey.worker.lib.osprey_shared.labels import LabelStatus
from osprey.worker.lib.storage.entity_label_webhook import EntityLabelWebhook
from osprey.worker.lib.storage.postgres import scoped_session
from osprey.worker.lib.webhooks import WebhookStatus
from sqlalchemy import func
from sqlalchemy.orm.session import Session


@pytest.fixture(autouse=True)
def sqlalchemy_session() -> Iterator[Session]:
    with scoped_session() as session:
        yield session


def _query_get_one(session: Session, webhook_id: int) -> EntityLabelWebhook:
    rtn = session.query(EntityLabelWebhook).filter_by(id=webhook_id).one()
    assert isinstance(rtn, EntityLabelWebhook)
    return rtn


def create() -> EntityLabelWebhook:
    webhook = EntityLabelWebhook()
    webhook.entity_type = 'dummy_entity_type'
    webhook.entity_id = '123456789'
    webhook.label_name = 'label_name'
    webhook.label_status = LabelStatus.ADDED
    webhook.webhook_name = 'webhook_name'
    webhook.claim_until = webhook.created_at = webhook.updated_at = func.now()
    webhook.status = WebhookStatus.QUEUED

    with scoped_session() as s:
        s.add(webhook)
        s.commit()
        return _query_get_one(s, webhook.id)


def test_claim__empty() -> None:
    assert EntityLabelWebhook.claim() is None


def test_claim__one() -> None:
    webhook = create()
    claimed = EntityLabelWebhook.claim()
    assert claimed is not None
    assert claimed.id == webhook.id


def test_claim__many() -> None:
    first = create()
    create()
    claimed = EntityLabelWebhook.claim()
    assert claimed is not None
    assert claimed.id == first.id


def test_release() -> None:
    create()
    webhook = EntityLabelWebhook.claim()
    assert webhook is not None

    webhook_id = webhook.id
    webhook.release(WebhookStatus.COMPLETE, 'result')

    with scoped_session() as s:
        updated = _query_get_one(s, webhook_id)

    assert updated.status == WebhookStatus.COMPLETE
    assert updated.result == 'result'
    assert updated.updated_at > updated.created_at
