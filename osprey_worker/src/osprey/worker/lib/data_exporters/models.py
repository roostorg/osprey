from osprey.worker.lib.data_exporters.constants import DATA_EVENT, EXPERIMENT_METADATA_UPDATE_EVENT
from pydantic import BaseModel


class ospreyExperimentMetadataUpdate(BaseModel):
    name: str = DATA_EVENT
    event_type: str = EXPERIMENT_METADATA_UPDATE_EVENT
    experiment: str
    buckets: list[str]
    bucket_sizes_v2: list[float]
    experiment_version: int
    experiment_revision: int
    rules_hash: str
    experiment_type: str
