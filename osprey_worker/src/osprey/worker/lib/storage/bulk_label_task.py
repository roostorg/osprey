from __future__ import absolute_import

import logging
import time
from datetime import datetime
from random import random
from typing import Any, Dict, Iterator, List, Optional

from osprey.engine.language_types.labels import LabelStatus
from osprey.rpc.labels.v1 import service_pb2
from osprey.worker.lib.storage.types import Enum
from sqlalchemy import BigInteger, Boolean, Column, DateTime, Integer, Text, and_, func, or_
from sqlalchemy.dialects.postgresql import ARRAY, INTERVAL, JSONB

from ..bulk_label import TaskStatus
from .postgres import Model, scoped_session

BASE_DELAY_SECONDS = 60 * 3
MAX_ATTEMPTS = 3

# This is short relative to the `BASE_DELAY_SECONDS` because the ui needs to update every second
HEARTBEAT_INTERVAL = 1.0

logger = logging.getLogger(__name__)


class BulkLabelTask(Model):
    __tablename__ = 'bulk_label_tasks'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    initiated_by = Column(Text, nullable=False)

    query = Column(JSONB, nullable=False)  # See osprey_ui_api/osprey/views/queries.py:93
    dimension = Column(Text, nullable=False)
    excluded_entities = Column(ARRAY(Text), nullable=False, default=[])
    expected_total_entities_to_label = Column(Integer, nullable=False)
    no_limit = Column(Boolean, nullable=False, default=False)

    label_name = Column(Text, nullable=False)
    label_status = Column(
        Enum(LabelStatus, name='label_status', create_type=False),
        nullable=False,
    )
    label_reason = Column(Text, nullable=False)
    label_expiry = Column(DateTime(timezone=True))

    task_status = Column(Enum(TaskStatus, name='task_status', create_type=False), nullable=False)
    entities_collected = Column(Integer, nullable=False, default=0)
    entities_labeled = Column(Integer, nullable=False, default=0)
    total_entities_to_label = Column(Integer, nullable=True)

    claim_until = Column(DateTime(timezone=True))
    result = Column(Text)
    attempts = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)

    @classmethod
    def enqueue(
        cls,
        query: Dict[str, Any],
        dimension: str,
        initiated_by: str,
        label_name: str,
        label_reason: str,
        label_status: 'service_pb2.LabelStatus.ValueType',  # NOTE: this could use regular LabelStatus, would just take a bit of refactoring
        label_expiry: Optional[datetime],
        excluded_entities: List[str],
        expected_total_entities_to_label: int,
        no_limit: bool,
    ) -> 'BulkLabelTask':
        with scoped_session(commit=True) as session:
            task = BulkLabelTask(
                initiated_by=initiated_by,
                query=query,
                dimension=dimension,
                excluded_entities=excluded_entities,
                expected_total_entities_to_label=expected_total_entities_to_label,
                no_limit=no_limit,
                label_name=label_name,
                label_reason=label_reason,
                label_status=LabelStatus(label_status),  # type: ignore
                label_expiry=label_expiry,
                task_status=TaskStatus.QUEUED,  # type: ignore
                claim_until=func.now(),  # type: ignore
                created_at=func.now(),  # type: ignore
                updated_at=func.now(),  # type: ignore
            )
            session.add(task)
        return task

    @classmethod
    def get_one(cls, task_id: int) -> Optional['BulkLabelTask']:
        with scoped_session() as session:
            task: Optional[BulkLabelTask] = session.query(BulkLabelTask).get(task_id)
            return task

    @classmethod
    def get_last_n(cls, last_n: int) -> List['BulkLabelTask']:
        table = cls.__table__
        query = table.select().limit(last_n).order_by(table.c.id.desc())
        with scoped_session() as session:
            return [cls(**result) for result in session.execute(query)]

    def serialize(self) -> Any:
        assert isinstance(self.query, dict)
        query_filter = self.query.get('query_filter')
        if not query_filter:
            query_filter = None

        assert self.created_at is not None
        assert self.updated_at is not None
        return {
            'task_id': self.id,
            'dimension': self.dimension,
            'entities_collected': self.entities_collected,
            'entities_labeled': self.entities_labeled,
            'total_entities_to_label': self.total_entities_to_label,
            'expected_total_entities_to_label': self.expected_total_entities_to_label,
            'task_status': self.task_status.name,  # type: ignore
            'attempts': self.attempts,
            'label_reason': self.label_reason,
            'initiated_by': self.initiated_by,
            'created_at': datetime.timestamp(self.created_at),
            'updated_at': datetime.timestamp(self.updated_at),
            'label_name': self.label_name,
            'no_limit': self.no_limit,
            'label_status': self.label_status.name,  # type: ignore
            'query_filter': query_filter,
            'query_start': self.query['start'],  # Stored as a POSIX already
            'query_end': self.query['end'],  # Stored as a POSIX already
        }

    def iterate_entity_indices(self) -> Iterator[int]:
        last_heartbeat_time = time.time()
        task_start_time = last_heartbeat_time

        assert self.total_entities_to_label is not None
        assert self.entities_labeled is not None
        for index in range(self.entities_labeled, self.total_entities_to_label):
            yield index
            if time.time() - last_heartbeat_time > HEARTBEAT_INTERVAL:
                # Set current_entity_index+1 so you don't repeat work
                self.heartbeat(status=TaskStatus.LABELLING, new_entity_count=index + 1)
                last_heartbeat_time = time.time()
                progress_pct = (float(self.entities_labeled) / float(self.total_entities_to_label)) * 100.0
                logging.info(
                    f'[task_id:{self.id}] task heartbeat success - task has been running for: '
                    f'{last_heartbeat_time - task_start_time} seconds '
                    f'[{self.entities_labeled}/{self.total_entities_to_label} ({progress_pct:.2f}%)]'
                )

        # We heartbeat at the end here with the full value `total_entities_to_label`
        # to update the ui to show 100% task completion
        self.heartbeat(status=TaskStatus.LABELLING, new_entity_count=self.total_entities_to_label)
        self.release(status=TaskStatus.COMPLETE)
        logging.info(
            f'[task_id:{self.id}] bulk label task completed successfully, labelling took: '
            f'{time.time() - task_start_time} seconds'
        )

    def heartbeat(
        self, status: TaskStatus, new_entity_count: int, claim_until_seconds: int = BASE_DELAY_SECONDS
    ) -> None:
        """
        Update the postgres task claim & update the entity count for the status type provided.
        """

        def _supplied_status_is_equal_to(expected_status: TaskStatus) -> bool:
            if status == expected_status:
                if self.task_status != expected_status:
                    self.task_status = expected_status
                return True
            return False

        with scoped_session(commit=True) as session:
            session.add(self)
            if _supplied_status_is_equal_to(TaskStatus.COLLECTING):
                self.entities_collected = new_entity_count
            elif _supplied_status_is_equal_to(TaskStatus.LABELLING):
                self.entities_labeled = new_entity_count
            # We use postgres time functions to keep the timezones consistent
            self.claim_until = func.now() + func.cast(func.concat(claim_until_seconds, ' SECONDS'), INTERVAL)

    @classmethod
    def claim(cls) -> Optional['BulkLabelTask']:
        """Claim one task to process."""
        table = cls.__table__

        def _calculate_lock_seconds(attempts: Any = 0) -> Any:
            """
            Helper method to calculate lock seconds with exponential backoff based on number of attempts.
            """
            jitter_percent = 1 + random()
            return func.cast(
                func.concat(func.pow(2, attempts) * BASE_DELAY_SECONDS * jitter_percent, ' SECONDS'), INTERVAL
            )

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
                    # Use enum.value because the enum RUNNING_DEPRECATED has a different literal value of 'RUNNING'
                    or_(*(table.c.task_status == status.value for status in TaskStatus.non_final_statuses())),
                    # we will include tasks that are equal to the max attempts just so we can close them out as
                    # failed in the bulk label sink. this should only happen if the sink is rebooted.
                    table.c.attempts <= MAX_ATTEMPTS,
                    or_(
                        and_(
                            # If the task is marked as retrying, wait until the last update was greater than
                            # the exponential backoff time to claim it again.
                            table.c.task_status == TaskStatus.RETRYING.value,
                            func.now() - table.c.updated_at > _calculate_lock_seconds(table.c.attempts),
                        ),
                        table.c.task_status != TaskStatus.RETRYING.value,
                    ),
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
                claim_until=func.now() + _calculate_lock_seconds(),
                attempts=table.c.attempts + 1,
                task_status=TaskStatus.COLLECTING.value,
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
            assert isinstance(row, BulkLabelTask)
            return row

    def release(self, status: TaskStatus, result: Optional[str] = None) -> None:
        with scoped_session(commit=True) as session:
            session.add(self)
            self.task_status = status
            self.result = result
            self.updated_at = func.now()
