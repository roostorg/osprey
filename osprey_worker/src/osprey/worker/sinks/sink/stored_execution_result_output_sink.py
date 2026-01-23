from osprey.engine.executor.execution_context import ExecutionResult
from osprey.worker.lib.storage.stored_execution_result import bootstrap_execution_result_storage_service
from osprey.worker.sinks.sink.output_sink import BaseOutputSink


class StoredExecutionResultOutputSink(BaseOutputSink):
    """An output sink that persists the execution result to an EventRecord."""

    # BigTable operations can be slow and may fail transiently.
    # Allow more time and retry on failure.
    timeout: float = 5.0
    max_retries: int = 2  # Up to 3 total attempts with exponential backoff

    def __init__(self):
        self._service = bootstrap_execution_result_storage_service()

    def will_do_work(self, result: ExecutionResult) -> bool:
        return True

    def push(self, result: ExecutionResult) -> None:
        self._service.persist_from_execution_result(result)

    def stop(self) -> None:
        pass
