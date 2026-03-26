from dataclasses import dataclass
from typing import Any

from osprey.engine.utils.types import add_slots

from .entities import EntityT
from .post_execution_convertible import PostExecutionConvertible

# TODO allow use of tuples as post execution convertibles
# Osprey UI doesn't recognize tuples. We will use a list for now.
# The content of the list will be strs with the keys denoted below by the commented out named tuple.
# ExperimentPostExecutionResultT = namedtuple(
#     'ExperimentPostExecutionResultT', [
#       'name', 'entity_id', 'entity_type',
#       'resolved_bucket', 'resolved_bucket_index', 'version'
# ]
# )


NOT_IN_EXPERIMENT_BUCKET = ''  # empty string bucket names means the entity is not in the experiment
NOT_IN_EXPERIMENT_BUCKET_INDEX = -1  # bucket index value for entities not in the experiment


@add_slots
@dataclass(frozen=True)
class ExperimentT(PostExecutionConvertible[list[str]]):
    name: str
    entity: EntityT[Any]
    buckets: list[str]
    bucket_sizes: list[float]
    resolved_bucket: str
    version: int
    revision: int

    def to_post_execution_value(self) -> list[str]:
        return [
            self.name,
            self.entity.id,
            self.entity.type,
            self.resolved_bucket,
            str(NOT_IN_EXPERIMENT_BUCKET_INDEX)
            if self.resolved_bucket is NOT_IN_EXPERIMENT_BUCKET
            else str(self.buckets.index(self.resolved_bucket)),
            str(self.version),
            str(self.revision),
        ]

    def __hash__(self) -> int:
        return hash(
            (
                self.name,
                self.entity,
                tuple(self.buckets),
                tuple(self.bucket_sizes),
                self.resolved_bucket,
                self.version,
                self.revision,
            )
        )
