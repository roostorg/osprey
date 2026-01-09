from __future__ import absolute_import

import logging
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any, Optional

from sqlalchemy import ARRAY, BigInteger, Column, DateTime, Enum, Integer, Text

from .postgres import Model, scoped_session

logger = logging.getLogger(__name__)


class BulkActionJobStatus(StrEnum):
    PENDING_UPLOAD = 'pending_upload'
    UPLOADED = 'uploaded'
    PARSING = 'parsing'
    PROCESSING = 'processing'
    COMPLETED = 'completed'
    FAILED = 'failed'
    CANCELLED = 'cancelled'


class BulkActionTaskStatus(StrEnum):
    PENDING = 'pending'
    PROCESSING = 'processing'
    COMPLETED = 'completed'
    FAILED = 'failed'
    CANCELLED = 'cancelled'


class BulkActionJob(Model):
    __tablename__ = 'bulk_action_jobs'

    id: int = Column(BigInteger, primary_key=True)
    user_id: str = Column(Text, nullable=False)
    status: BulkActionJobStatus = Column(
        Enum(BulkActionJobStatus, name='job_status', create_type=True, values_callable=lambda x: [e.value for e in x]),
        nullable=False,
    )
    gcs_path: str = Column(Text, nullable=False)
    original_filename: str = Column(Text, nullable=False)
    total_rows: int = Column(Integer, nullable=False)
    processed_rows: int = Column(Integer, nullable=False)
    error: Optional[str] = Column(Text)
    action_workflow_name: str = Column(Text, nullable=False)
    entity_type: str = Column(Text, nullable=False)
    created_at: datetime = Column(DateTime(timezone=True), nullable=False)
    updated_at: datetime = Column(DateTime(timezone=True), nullable=False)
    name: str = Column(Text, nullable=False)
    description: str = Column(Text, nullable=False)

    def create_task(self, task_id: int, chunk_number: int, row_offset: int, row_count: int) -> 'BulkActionTask':
        with scoped_session(commit=True) as session:
            task = BulkActionTask(
                id=task_id,
                job_id=self.id,
                chunk_number=chunk_number,
                row_offset=row_offset,
                row_count=row_count,
                status=BulkActionTaskStatus.PENDING,
                created_at=datetime.now(timezone.utc),
                completed_at=None,
                error=None,
                attempts=0,
                failed_row_offsets=[],
            )
            session.add(task)
            return task

    @classmethod
    def create_job(
        cls,
        job_id: int,
        user_id: str,
        gcs_path: str,
        original_filename: str,
        total_rows: int,
        action_workflow_name: str,
        entity_type: str,
        name: str,
        description: str,
    ) -> 'BulkActionJob':
        with scoped_session(commit=True) as session:
            job = BulkActionJob(
                id=job_id,
                user_id=user_id,
                gcs_path=gcs_path,
                original_filename=original_filename,
                total_rows=total_rows,
                processed_rows=0,
                action_workflow_name=action_workflow_name,
                entity_type=entity_type,
                status=BulkActionJobStatus.PENDING_UPLOAD,
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
                name=name,
                description=description,
            )
            session.add(job)
        return job

    @classmethod
    def get_one(cls, job_id: int) -> Optional['BulkActionJob']:
        with scoped_session() as session:
            return session.query(cls).filter(cls.id == job_id).first()

    @classmethod
    def get_all(cls) -> list['BulkActionJob']:
        with scoped_session() as session:
            return session.query(cls).all()

    @classmethod
    def get_all_jobs_by_status(cls, status: BulkActionJobStatus) -> list['BulkActionJob']:
        with scoped_session() as session:
            return session.query(cls).filter(cls.status == status).all()

    @classmethod
    def get_next_file_process_job(cls) -> Optional['BulkActionJob']:
        with scoped_session() as session:
            return (
                session.query(cls).filter(cls.status == BulkActionJobStatus.UPLOADED).order_by(cls.created_at).first()
            )

    def cancel(self):
        with scoped_session(commit=True) as session:
            self.status = BulkActionJobStatus.CANCELLED

            for task in self.get_all_tasks():
                task.status = BulkActionTaskStatus.CANCELLED
                task.error = 'Job cancelled'
                task.completed_at = datetime.now(timezone.utc)
                session.merge(task)

            session.merge(self)

    def update_job(self, **kwargs):
        with scoped_session(commit=True) as session:
            for key, value in kwargs.items():
                setattr(self, key, value)

            session.merge(self)

    def get_all_tasks(self) -> list['BulkActionTask']:
        with scoped_session() as session:
            return session.query(BulkActionTask).filter(BulkActionTask.job_id == self.id).all()

    def serialize(self) -> dict[str, Any]:
        return {
            'id': str(self.id),
            'status': self.status,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'gcs_path': self.gcs_path,
            'original_filename': self.original_filename,
            'total_rows': self.total_rows,
            'processed_rows': self.processed_rows,
            'error': self.error,
            'action_workflow_name': self.action_workflow_name,
            'entity_type': self.entity_type,
            'user_id': self.user_id,
            'description': self.description,
            'name': self.name,
        }


class BulkActionTask(Model):
    __tablename__ = 'bulk_action_job_tasks'

    id: int = Column(BigInteger, primary_key=True)
    job_id: int = Column(BigInteger, nullable=False)
    status: BulkActionTaskStatus = Column(
        Enum(
            BulkActionTaskStatus, name='task_status', create_type=False, values_callable=lambda x: [e.value for e in x]
        ),
        nullable=False,
    )
    chunk_number: int = Column(Integer, nullable=False)
    row_offset: int = Column(Integer, nullable=False)
    row_count: int = Column(Integer, nullable=False)
    error: Optional[str] = Column(Text)
    attempts: int = Column(Integer, nullable=False)
    failed_row_offsets = Column(ARRAY(Integer), nullable=False)
    created_at: datetime = Column(DateTime(timezone=True), nullable=False)
    completed_at: Optional[datetime] = Column(DateTime(timezone=True))

    @classmethod
    def get_one(cls, task_id: int) -> Optional['BulkActionTask']:
        with scoped_session() as session:
            return session.query(cls).filter(cls.id == task_id).first()

    @classmethod
    def get_all_by_job_id_for_status(cls, job_id: int, status: BulkActionTaskStatus) -> list['BulkActionTask']:
        with scoped_session() as session:
            return session.query(cls).filter(cls.job_id == job_id, cls.status == status).all()

    @classmethod
    def get_next_pending_task(cls, job_id: int) -> Optional['BulkActionTask']:
        with scoped_session() as session:
            return (
                session.query(cls)
                .filter(cls.job_id == job_id, cls.status == BulkActionTaskStatus.PENDING)
                .order_by(cls.created_at)
                .first()
            )

    def update_task(self, **kwargs):
        with scoped_session(commit=True) as session:
            for key, value in kwargs.items():
                setattr(self, key, value)
            session.merge(self)
