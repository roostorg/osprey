from enum import StrEnum, auto


class ExecutionResultStoreType(StrEnum):
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
    Execution result store that is sdefined via register_execution_result_store
    """

    NONE = auto()
    """
    Disable execution results from being stored
    """
