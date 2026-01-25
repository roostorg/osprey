from datetime import datetime
from typing import Optional

from osprey.worker.ui_api.osprey.lib.druid import TopNDruidQuery


class BulkLabelTopNRequest(TopNDruidQuery):
    excluded_entities: list[str] = []
    expected_entities: int
    no_limit: bool
    label_name: str
    label_status: str
    label_reason: str
    label_expiry: Optional[datetime]
