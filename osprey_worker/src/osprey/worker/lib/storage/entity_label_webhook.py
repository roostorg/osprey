from __future__ import absolute_import

import logging
from random import random
from typing import Optional

from osprey.worker.sinks.sink.output_sink_utils.models import LabelStatus
from sqlalchemy import BigInteger, Column, DateTime, Integer, Text, and_, func, or_
from sqlalchemy.dialects.postgresql import INTERVAL, JSONB

from ..webhooks import WebhookStatus
from .postgres import Model, scoped_session
from .types import Enum

BASE_DELAY_SECONDS = 60
MAX_ATTEMPTS = 3  # update table index in osprey/osprey_lib/schemas/osprey.sql if we change this value
logger = logging.getLogger(__name__)


class EntityLabelWebhook(Model):
    __tablename__ = 'entity_label_webhooks'

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    entity_type = Column(Text, nullable=False)
    entity_id = Column(Text, nullable=False)
    label_name = Column(Text, nullable=False)
    label_status = Column(Enum(LabelStatus, name='label_status', create_type=False), nullable=False)
    webhook_name = Column(Text, nullable=False)
    arguments = Column(JSONB)
    features = Column(JSONB)
    status = Column(Enum(WebhookStatus, name='webhook_status', create_type=False))
    claim_until = Column(DateTime(timezone=True))
    result = Column(Text)
    attempts = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)

    @classmethod
    def claim(cls) -> Optional['EntityLabelWebhook']:
        """Claim one webhook to send.

        The claim duration is also used as the retry cooldown, since that's already longer than we expect sending a
        webhook to take. That way, if the process totally dies, the webhook can still be retried at the correct
        interval.
        """
        table = cls.__table__
        jitter_percent = 1 + random()
        lock_seconds = BASE_DELAY_SECONDS * func.power(2, table.c.attempts) * jitter_percent

        # Selects the oldest claimable row's id in a subquery, because UPDATE doesn't support ORDER BY.
        # - oldest is based on claim_until (which is initially set to created_at, or some other time if it's a
        #       delayed action).
        # - claimable means:
        #   - the claim has expired
        #   - status is one of the non-final statuses
        #   - it hasn't already been attempted too many times
        order_subq = (
            table.select()
            .with_only_columns([table.c.id])
            .where(
                and_(
                    table.c.claim_until < func.now(),
                    or_(*(table.c.status == status for status in WebhookStatus.non_final_statuses())),
                    table.c.attempts < MAX_ATTEMPTS,
                )
            )
            .with_for_update(skip_locked=True)
            .order_by(table.c.claim_until)
            .limit(1)
            .alias('order_subq')
        )

        query = (
            table.update()
            .where(table.c.id.in_(order_subq))
            .values(
                claim_until=func.now() + func.cast(func.concat(lock_seconds, ' SECONDS'), INTERVAL),
                attempts=table.c.attempts + 1,
                status=WebhookStatus.RUNNING,
                updated_at=func.now(),
            )
            .returning(table)
        )

        with scoped_session(commit=True) as session:
            cursor = session.execute(query)
            # We need to construct the ORM object in a way SQLAlchemy approves of so it can track state under the
            # hood correctly (namely know that this object represents an existing row in the database).
            rows = list(session.query(cls).instances(cursor))
            if len(rows) == 0:
                return None
            # We should only ever match up to one row
            (row,) = rows
            assert isinstance(row, EntityLabelWebhook)
            return row

    def release(self, status: WebhookStatus, result: str) -> None:
        with scoped_session(commit=True) as session:
            session.add(self)
            self.status = status
            self.result = result
            self.updated_at = func.now()
