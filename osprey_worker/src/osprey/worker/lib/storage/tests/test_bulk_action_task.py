from typing import Iterator
from unittest.mock import patch

import pytest
from osprey.worker.lib.snowflake import generate_snowflake
from osprey.worker.lib.storage.bulk_action_task import (
    BulkActionJob,
    BulkActionJobStatus,
    BulkActionTask,
    BulkActionTaskStatus,
)
from osprey.worker.lib.storage.postgres import scoped_session
from sqlalchemy.orm import Session


@pytest.fixture
def mock_session():
    with patch('osprey.worker.lib.storage.bulk_action_task.scoped_session') as mock:
        session = mock.return_value.__enter__.return_value
        yield session


@pytest.fixture(autouse=True)
def sqlalchemy_session() -> Iterator[Session]:
    with scoped_session() as session:
        yield session


def test_create_job(sqlalchemy_session):
    user_id = '123'
    gcs_path = 'gcs://bucket/path'
    original_file_name = 'test.csv'
    total_rows = 100
    action_workflow_name = 'test_workflow'
    entity_type = 'user'
    name = 'test_name'
    description = 'test_description'
    job_id = generate_snowflake().to_int()
    job = BulkActionJob.create_job(
        job_id=job_id,
        user_id=user_id,
        gcs_path=gcs_path,
        original_filename=original_file_name,
        total_rows=total_rows,
        action_workflow_name=action_workflow_name,
        entity_type=entity_type,
        name=name,
        description=description,
    )

    assert job.id == job_id
    assert job.user_id == user_id
    assert job.gcs_path == gcs_path
    assert job.original_filename == original_file_name
    assert job.total_rows == total_rows
    assert job.processed_rows is None
    assert job.action_workflow_name == action_workflow_name
    assert job.entity_type == entity_type
    assert job.status == BulkActionJobStatus.PENDING_UPLOAD


def test_get_one_bulk_action_job(sqlalchemy_session):
    job = BulkActionJob.create_job(
        job_id=generate_snowflake().to_int(),
        user_id='123',
        gcs_path='gs://bucket/path',
        original_filename='test.csv',
        total_rows=100,
        action_workflow_name='test_workflow',
        entity_type='user',
        name='test_name',
        description='test_description',
    )

    result = BulkActionJob.get_one(job.id)

    assert result == job


def test_create_bulk_action_task(sqlalchemy_session):
    job = BulkActionJob.create_job(
        job_id=generate_snowflake().to_int(),
        user_id='123',
        gcs_path='gs://bucket/path',
        original_filename='test.csv',
        total_rows=100,
        action_workflow_name='test_workflow',
        entity_type='user',
        name='test_name',
        description='test_description',
    )
    chunk_number = 1
    row_offset = 0
    row_count = 1000

    task = job.create_task(
        task_id=generate_snowflake().to_int(),
        chunk_number=chunk_number,
        row_offset=row_offset,
        row_count=row_count,
    )

    assert task.job_id == job.id
    assert task.chunk_number == chunk_number
    assert task.row_offset == row_offset
    assert task.row_count == row_count
    assert task.status == BulkActionTaskStatus.PENDING
    assert task.attempts == 0
    assert task.failed_row_offsets == []
    assert task.error is None
    assert task.completed_at is None
    assert task.created_at is not None


def test_create_bulk_action_task_persists_to_db(sqlalchemy_session):
    job = BulkActionJob.create_job(
        job_id=generate_snowflake().to_int(),
        user_id='123',
        gcs_path='gs://bucket/path',
        original_filename='test.csv',
        total_rows=100,
        action_workflow_name='test_workflow',
        entity_type='user',
        name='test_name',
        description='test_description',
    )
    chunk_number = 1
    row_offset = 0
    row_count = 1000

    task = job.create_task(
        task_id=generate_snowflake().to_int(),
        chunk_number=chunk_number,
        row_offset=row_offset,
        row_count=row_count,
    )

    # Query directly from DB to verify persistence
    persisted_task = sqlalchemy_session.query(BulkActionTask).filter(BulkActionTask.id == task.id).first()

    assert persisted_task is not None
    assert persisted_task.job_id == job.id
    assert persisted_task.chunk_number == chunk_number
    assert persisted_task.row_offset == row_offset
    assert persisted_task.row_count == row_count
    assert persisted_task.status == BulkActionTaskStatus.PENDING


def test_get_one_task(sqlalchemy_session):
    job = BulkActionJob.create_job(
        job_id=generate_snowflake().to_int(),
        user_id='123',
        gcs_path='gs://bucket/path',
        original_filename='test.csv',
        total_rows=100,
        action_workflow_name='test_workflow',
        entity_type='user',
        name='test_name',
        description='test_description',
    )
    task = job.create_task(
        task_id=generate_snowflake().to_int(),
        chunk_number=1,
        row_offset=0,
        row_count=1000,
    )

    result = BulkActionTask.get_one(task.id)

    assert result is not None
    assert result.id == task.id
    assert result.job_id == job.id
    assert result.chunk_number == task.chunk_number


def test_get_one_task_not_found(sqlalchemy_session):
    result = BulkActionTask.get_one(999999)

    assert result is None


def test_get_all_by_job_id(sqlalchemy_session):
    job = BulkActionJob.create_job(
        job_id=generate_snowflake().to_int(),
        user_id='123',
        gcs_path='gs://bucket/path',
        original_filename='test.csv',
        total_rows=100,
        action_workflow_name='test_workflow',
        entity_type='user',
        name='test_name',
        description='test_description',
    )

    job.create_task(task_id=generate_snowflake().to_int(), chunk_number=1, row_offset=0, row_count=1000)
    job.create_task(task_id=generate_snowflake().to_int(), chunk_number=2, row_offset=1000, row_count=1000)
    job.create_task(task_id=generate_snowflake().to_int(), chunk_number=3, row_offset=2000, row_count=1000)

    other_job = BulkActionJob.create_job(
        job_id=generate_snowflake().to_int(),
        user_id='456',
        gcs_path='gs://bucket/path2',
        original_filename='test2.csv',
        total_rows=100,
        action_workflow_name='test_workflow',
        entity_type='user',
        name='test_name',
        description='test_description',
    )
    other_job.create_task(task_id=generate_snowflake().to_int(), chunk_number=1, row_offset=0, row_count=1000)

    result = BulkActionTask.get_all_by_job_id_for_status(job.id, BulkActionTaskStatus.PENDING)

    assert len(result) == 3
    assert all(task.job_id == job.id for task in result)
    assert {task.chunk_number for task in result} == {1, 2, 3}


def test_update_task(sqlalchemy_session):
    job = BulkActionJob.create_job(
        job_id=generate_snowflake().to_int(),
        user_id='123',
        gcs_path='gs://bucket/path',
        original_filename='test.csv',
        total_rows=100,
        action_workflow_name='test_workflow',
        entity_type='user',
        name='test_name',
        description='test_description',
    )
    task = job.create_task(
        task_id=generate_snowflake().to_int(),
        chunk_number=1,
        row_offset=0,
        row_count=1000,
    )

    task.update_task(status=BulkActionTaskStatus.PROCESSING, attempts=1, error='test error')

    assert task.status == BulkActionTaskStatus.PROCESSING
    assert task.attempts == 1
    assert task.error == 'test error'

    persisted_task = BulkActionTask.get_one(task.id)
    assert persisted_task is not None
    assert persisted_task.status == BulkActionTaskStatus.PROCESSING
    assert persisted_task.attempts == 1
    assert persisted_task.error == 'test error'


def test_get_next_pending_task(sqlalchemy_session):
    job = BulkActionJob.create_job(
        job_id=generate_snowflake().to_int(),
        user_id='123',
        gcs_path='gs://bucket/path',
        original_filename='test.csv',
        total_rows=100,
        action_workflow_name='test_workflow',
        entity_type='user',
        name='test_name',
        description='test_description',
    )
    task1 = job.create_task(task_id=generate_snowflake().to_int(), chunk_number=3, row_offset=2000, row_count=1000)
    task2 = job.create_task(task_id=generate_snowflake().to_int(), chunk_number=1, row_offset=0, row_count=1000)

    task1.update_task(status=BulkActionTaskStatus.PROCESSING)

    next_task = BulkActionTask.get_next_pending_task(job.id)

    assert next_task is not None
    assert next_task.id == task2.id
    assert next_task.status == BulkActionTaskStatus.PENDING


def test_get_next_pending_task_no_pending_tasks(sqlalchemy_session):
    job = BulkActionJob.create_job(
        job_id=generate_snowflake().to_int(),
        user_id='123',
        gcs_path='gs://bucket/path',
        original_filename='test.csv',
        total_rows=100,
        action_workflow_name='test_workflow',
        entity_type='user',
        name='test_name',
        description='test_description',
    )
    task = job.create_task(task_id=generate_snowflake().to_int(), chunk_number=1, row_offset=0, row_count=1000)
    task.update_task(status=BulkActionTaskStatus.COMPLETED)

    next_task = BulkActionTask.get_next_pending_task(job.id)

    assert next_task is None


def test_get_next_pending_task_different_jobs(sqlalchemy_session):
    job1 = BulkActionJob.create_job(
        job_id=generate_snowflake().to_int(),
        user_id='123',
        gcs_path='gs://bucket/path1',
        original_filename='test1.csv',
        total_rows=100,
        action_workflow_name='test_workflow',
        entity_type='user',
        name='test_name',
        description='test_description',
    )
    job2 = BulkActionJob.create_job(
        job_id=generate_snowflake().to_int(),
        user_id='456',
        gcs_path='gs://bucket/path2',
        original_filename='test2.csv',
        total_rows=100,
        action_workflow_name='test_workflow',
        entity_type='user',
        name='test_name',
        description='test_description',
    )

    task1 = job1.create_task(task_id=generate_snowflake().to_int(), chunk_number=1, row_offset=0, row_count=1000)
    task2 = job2.create_task(task_id=generate_snowflake().to_int(), chunk_number=1, row_offset=0, row_count=1000)

    next_task_1 = BulkActionTask.get_next_pending_task(job1.id)
    next_task_2 = BulkActionTask.get_next_pending_task(job2.id)

    assert next_task_1 is not None
    assert next_task_2 is not None
    assert next_task_1.id == task1.id
    assert next_task_2.id == task2.id
