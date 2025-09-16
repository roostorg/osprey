from enum import Enum
from typing import Collection


class WebhookStatus(Enum):
    QUEUED = 'QUEUED'
    RUNNING = 'RUNNING'
    COMPLETE = 'COMPLETE'
    SKIPPED = 'SKIPPED'
    FAILED = 'FAILED'

    @staticmethod
    def final_statuses() -> Collection['WebhookStatus']:
        return {WebhookStatus.COMPLETE, WebhookStatus.SKIPPED, WebhookStatus.FAILED}

    @staticmethod
    def non_final_statuses() -> Collection['WebhookStatus']:
        return {status for status in WebhookStatus if not status.is_final()}

    def is_final(self) -> bool:
        return self in WebhookStatus.final_statuses()
