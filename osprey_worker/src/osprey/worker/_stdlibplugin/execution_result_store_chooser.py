from typing import Optional

from osprey.worker.adaptor.plugin_manager import bootstrap_execution_result_store
from osprey.worker.lib.singletons import CONFIG
from osprey.worker.lib.storage import ExecutionResultStoreType
from osprey.worker.lib.storage.stored_execution_result import (
    ExecutionResultStore,
    StoredExecutionResultBigTable,
    StoredExecutionResultGCS,
    StoredExecutionResultMinIO,
)


def get_rules_execution_result_store(
    execution_result_store_type: ExecutionResultStoreType,
) -> Optional[ExecutionResultStore]:
    """Based on the `execution_result_store_type` constructs a configured execution result store that can be used to store execution
    results. For more details, see `ExecutionResultStore`."""

    config = CONFIG.instance()

    if execution_result_store_type == ExecutionResultStoreType.BIGTABLE:
        return StoredExecutionResultBigTable()
    elif execution_result_store_type == ExecutionResultStoreType.GCS:
        return StoredExecutionResultGCS()
    elif execution_result_store_type == ExecutionResultStoreType.MINIO:
        endpoint = config.get_str('OSPREY_MINIO_ENDPOINT', 'minio:9000')
        access_key = config.get_str('OSPREY_MINIO_ACCESS_KEY', 'minioadmin')
        secret_key = config.get_str('OSPREY_MINIO_SECRET_KEY', 'minioadmin123')
        secure = config.get_bool('OSPREY_MINIO_SECURE', False)
        bucket_name = config.get_str('OSPREY_MINIO_EXECUTION_RESULTS_BUCKET', 'execution-output')

        return StoredExecutionResultMinIO(
            endpoint=endpoint, access_key=access_key, secret_key=secret_key, secure=secure, bucket_name=bucket_name
        )
    elif execution_result_store_type == ExecutionResultStoreType.PLUGIN:
        store = bootstrap_execution_result_store(config=config)
        if store is None:
            raise AssertionError('No execution result store registered')
    elif execution_result_store_type == ExecutionResultStoreType.NONE:
        return None

    return None
