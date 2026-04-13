from typing import Optional

from osprey.worker.adaptor.plugin_manager import bootstrap_execution_result_store
from osprey.worker.lib.config import Config
from osprey.worker.lib.singletons import CONFIG
from osprey.worker.lib.storage import ExecutionResultStorageBackendType
from osprey.worker.lib.storage.stored_execution_result import (
    ExecutionResultStore,
    StoredExecutionResultBigTable,
    StoredExecutionResultGCS,
    StoredExecutionResultMinIO,
    StoredExecutionResultPostgres,
)


def get_configured_execution_result_storage_backend_type(
    config: Config,
) -> Optional[ExecutionResultStorageBackendType]:
    configured_backend = config.get_optional_str('OSPREY_EXECUTION_RESULT_STORAGE_BACKEND')
    if configured_backend is None:
        return None

    return ExecutionResultStorageBackendType(configured_backend.lower())


def get_rules_execution_result_storage_backend(
    backend_type: ExecutionResultStorageBackendType,
) -> Optional[ExecutionResultStore]:
    """Based on the `backend_type` constructs a configured execution result store that can be used to store execution
    results. For more details, see `ExecutionResultStore`."""

    config = CONFIG.instance()

    if backend_type == ExecutionResultStorageBackendType.BIGTABLE:
        return StoredExecutionResultBigTable()
    elif backend_type == ExecutionResultStorageBackendType.GCS:
        return StoredExecutionResultGCS()
    elif backend_type == ExecutionResultStorageBackendType.MINIO:
        endpoint = config.get_str('OSPREY_MINIO_ENDPOINT', 'minio:9000')
        access_key = config.get_str('OSPREY_MINIO_ACCESS_KEY', 'minioadmin')
        secret_key = config.get_str('OSPREY_MINIO_SECRET_KEY', 'minioadmin123')
        secure = config.get_bool('OSPREY_MINIO_SECURE', False)
        bucket_name = config.get_str('OSPREY_MINIO_EXECUTION_RESULTS_BUCKET', 'execution-output')

        return StoredExecutionResultMinIO(
            endpoint=endpoint, access_key=access_key, secret_key=secret_key, secure=secure, bucket_name=bucket_name
        )
    elif backend_type == ExecutionResultStorageBackendType.POSTGRES:
        return StoredExecutionResultPostgres()
    elif backend_type == ExecutionResultStorageBackendType.PLUGIN:
        store = bootstrap_execution_result_store(config=config)
        if store is None:
            raise AssertionError('No execution result store registered')
        return store
    elif backend_type == ExecutionResultStorageBackendType.NONE:
        return None

    return None
