from __future__ import absolute_import

from datetime import datetime, timedelta
from typing import Iterator

import pytest
from osprey.rpc.labels.v1.service_pb2 import LabelStatus
from osprey.worker.lib.bulk_label import TaskStatus
from osprey.worker.lib.storage.bulk_label_task import BASE_DELAY_SECONDS, BulkLabelTask
from osprey.worker.lib.storage.postgres import scoped_session
from sqlalchemy.orm import Session


@pytest.fixture(autouse=True)
def sqlalchemy_session() -> Iterator[Session]:
    with scoped_session() as session:
        yield session


@pytest.fixture(autouse=True)
def wipe_db(sqlalchemy_session: Session) -> None:
    with scoped_session(commit=True) as session:
        session.execute('TRUNCATE TABLE bulk_label_tasks')
        print('truncating table')


def _query_get_one(task_id: int) -> BulkLabelTask:
    with scoped_session() as session:
        rtn = session.query(BulkLabelTask).filter_by(id=task_id).one()
        assert isinstance(rtn, BulkLabelTask)
        return rtn


def enqueue() -> BulkLabelTask:
    task = BulkLabelTask.enqueue(
        initiated_by='test@test.com',
        query={
            'query_filter': 'fake',
            'start': datetime.timestamp(datetime.now()),
            'end': datetime.timestamp(datetime.now()),
        },
        excluded_entities=[],
        expected_total_entities_to_label=10,
        no_limit=False,
        dimension='UserId',
        label_name='fake',
        label_reason='fake',
        label_status=LabelStatus.MANUALLY_ADDED,
        label_expiry=None,
    )

    return task


def test_claim__empty() -> None:
    assert BulkLabelTask.claim() is None


def test_enqueue() -> None:
    task = enqueue()
    db_task = _query_get_one(task.id)
    assert db_task.id == task.id


def test_claim__one() -> None:
    task = enqueue()
    claimed = BulkLabelTask.claim()
    assert claimed is not None
    assert claimed.id == task.id
    assert claimed.attempts == 1


def test_claim__many() -> None:
    first = enqueue()
    second = enqueue()
    claimed = BulkLabelTask.claim()
    assert claimed is not None
    assert claimed.id == first.id
    second_claimed = BulkLabelTask.claim()
    assert second_claimed is not None
    assert second_claimed.id == second.id


def test_release() -> None:
    enqueue()
    task = BulkLabelTask.claim()
    assert task is not None

    task.release(status=TaskStatus.COMPLETE, result='result')
    updated = _query_get_one(task.id)

    assert updated.task_status == TaskStatus.COMPLETE
    assert updated.result == 'result'
    assert updated.updated_at > updated.created_at


def test_claim_time() -> None:
    enqueue()
    # Get the database time to avoid any future timezone issues
    with scoped_session() as session:
        (now,) = session.execute('SELECT NOW();').first()
    task = BulkLabelTask.claim()
    assert task is not None
    assert task.claim_until >= now + timedelta(seconds=BASE_DELAY_SECONDS)


def test_get_one() -> None:
    task = enqueue()
    assert task is not None
    same_task = BulkLabelTask.get_one(task.id)
    assert same_task is not None
    assert task.id == same_task.id
