from typing import Optional

from osprey.worker._stdlibplugin.execution_result_store_chooser import (
    get_configured_execution_result_storage_backend_type,
    get_rules_execution_result_storage_backend,
)
from osprey.worker.adaptor.plugin_manager import hookimpl_osprey
from osprey.worker.lib.config import Config
from osprey.worker.lib.storage import ExecutionResultStorageBackendType
from osprey.worker.lib.storage.stored_execution_result import ExecutionResultStore


@hookimpl_osprey(trylast=True)
def register_execution_result_store(config: Config) -> Optional[ExecutionResultStore]:
    storage_backend_type = get_configured_execution_result_storage_backend_type(config)

    if storage_backend_type is None:
        return None

    if storage_backend_type == ExecutionResultStorageBackendType.PLUGIN:
        return None

    storage_backend = get_rules_execution_result_storage_backend(backend_type=storage_backend_type)

    return storage_backend
