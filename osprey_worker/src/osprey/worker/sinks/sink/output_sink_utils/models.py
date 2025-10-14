from typing import List, Optional

from pydantic import BaseModel

from .constants import OspreyAnalyticsEvents


class OspreyBulkJobAnalyticsEvent(BaseModel):
    name: str = 'event'
    event_type: str = OspreyAnalyticsEvents.BULK_LABEL_JOB
    task_id: str
    total_entities_to_label: int
    label_name: str
    label_reason: str
    no_limit: bool
    expiration_date: Optional[str]


class OspreyRulesVisualizerGenGraphAnalyticsEvent(BaseModel):
    name: str = 'event'
    event_type: str = OspreyAnalyticsEvents.RULES_VISUALIZER_GEN_GRAPH
    source_features: List[str]
    path: str
    request_method: str
    timestamp: str
