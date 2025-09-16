from datetime import datetime
from typing import Any, Dict, List, Optional

from osprey.worker.lib.osprey_shared.labels import LabelStatus
from pydantic import BaseModel

from .constants import MutationEventType, OspreyAnalyticsEvents


class OspreyLabelMutationAnalyticsEvent(BaseModel):
    name: str = 'event'
    event_type: str = OspreyAnalyticsEvents.LABEL_MUTATIONS
    mutation_event_type: MutationEventType
    mutation_event_id: str
    mutation_event_action_name: Optional[str] = None
    user_id: Optional[int]
    entity_id_v2: str
    entity_type: str
    labels: List[str]
    label_statuses: List[str]
    label_reasons: List[str]


class OspreyEntityLabelWebhook(BaseModel):
    entity_type: str
    entity_id: str
    label_name: str
    label_status: LabelStatus
    webhook_name: str
    features: Dict[str, Any]
    created_at: datetime


class OspreyActionClassificationAnalyticsEvent(BaseModel):
    name: str = 'event'
    event_type: str = OspreyAnalyticsEvents.ACTION_CLASSIFICATION
    action_name: str
    action_id: str
    action_timestamp: str


class OspreyExtractedFeaturesAnalyticsEvent(BaseModel):
    name: str = 'event'
    event_type: str = OspreyAnalyticsEvents.EXTRACTED_FEATURES
    action_name: str
    action_id: str
    action_timestamp: str
    user_id: Optional[int]
    error_count: int
    extracted_features_json: Dict[str, Any]


class OspreyExperimentExposureEvent(BaseModel):
    name: str = 'event'
    event_type: str = OspreyAnalyticsEvents.EXPERIMENT_EXPOSURE_EVENT
    experiment: str
    user_id: Optional[int]
    entity_id_v2: str
    entity_type: str
    bucket_name: str
    bucket_index: int
    action_id: str
    event: str = 'experiment_osprey_triggered'  # follows experimentation platform naming convention
    experiment_version: int
    experiment_revision: int
    guild_id: Optional[int]
    action_timestamp: str


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


class OspreyExecutionResultBigQueryPubsubEvent(BaseModel):
    action_id: str
    action_name: str
    timestamp: datetime
    error_count: int
    sample_rate: int
    classifications: List[str]
    signals: List[str]
    entity_label_mutations: List[str]
    error_results: Optional[str]
    execution_results: str
