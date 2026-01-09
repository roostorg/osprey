from enum import StrEnum, auto

# Import all models to ensure they're registered with SQLAlchemy
# This is required for metadata.create_all() to create all tables
from .bulk_action_task import BulkActionJob, BulkActionTask  # noqa: F401
from .bulk_label_task import BulkLabelTask  # noqa: F401
from .queries import Query, SavedQuery  # noqa: F401
from .temporary_ability_token import TemporaryAbilityToken  # noqa: F401


class ExecutionResultStorageBackendType(StrEnum):
    """Type of store used for execution results."""

    BIGTABLE = auto()
    """
    Bigtable execution result store
    """

    GCS = auto()
    """
    Google Cloud Storage execution result store
    """

    MINIO = auto()
    """
    Minio execution result store
    """

    PLUGIN = auto()
    """
    Execution result store that is defined via register_execution_result_store
    """

    NONE = auto()
    """
    Disable execution results from being stored. This may cause certain elements of Osprey to break, such as the events stream and individual event details in the UI
    """
