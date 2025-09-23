from osprey.worker.adaptor.plugin_manager import hookimpl_osprey
from osprey.worker.lib.config import Config
from osprey.worker.lib.storage.stored_execution_result import ExecutionResultStore, StoredExecutionResultMinIO


@hookimpl_osprey
def register_execution_result_store(config: Config) -> ExecutionResultStore:
    endpoint = config.get_str('OSPREY_MINIO_ENDPOINT', 'minio:9000')
    access_key = config.get_str('OSPREY_MINIO_ACCESS_KEY', 'minioadmin')
    secret_key = config.get_str('OSPREY_MINIO_SECRET_KEY', 'minioadmin123')
    secure = config.get_bool('OSPREY_MINIO_SECURE', False)
    bucket_name = config.get_str('OSPREY_MINIO_EXECUTION_RESULTS_BUCKET', 'execution-output')

    return StoredExecutionResultMinIO(
        endpoint=endpoint, access_key=access_key, secret_key=secret_key, secure=secure, bucket_name=bucket_name
    )
