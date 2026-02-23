from collections.abc import Collection
from enum import Enum


class TaskStatus(Enum):
    QUEUED = 'QUEUED'
    RUNNING_DEPRECATED = (
        'RUNNING'  # **DEPRECATED** - Please use COLLECTING / LABELLING to more clearly indicate the status!
    )
    COLLECTING = 'COLLECTING'
    LABELLING = 'LABELLING'
    COMPLETE = 'COMPLETE'
    RETRYING = 'RETRYING'
    FAILED = 'FAILED'

    @staticmethod
    def final_statuses() -> Collection['TaskStatus']:
        return {TaskStatus.COMPLETE, TaskStatus.FAILED}

    @staticmethod
    def non_final_statuses() -> Collection['TaskStatus']:
        return {status for status in TaskStatus if not status.is_final() and status != TaskStatus.RUNNING_DEPRECATED}

    def is_final(self) -> bool:
        return self in TaskStatus.final_statuses()
