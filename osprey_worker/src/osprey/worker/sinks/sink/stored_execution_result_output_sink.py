from osprey.engine.executor.execution_context import ExecutionResult
from osprey.worker.lib.storage.stored_execution_result import bootstrap_execution_result_storage_service
from osprey.worker.sinks.sink.output_sink import BaseOutputSink


class StoredExecutionResultOutputSink(BaseOutputSink):
    """An output sink that persists the execution result to an EventRecord."""

    # BigTable writes can take longer than the default 2s, especially on first connection
    timeout: float = 10.0

    def __init__(self):
        self._service = bootstrap_execution_result_storage_service()

    def will_do_work(self, result: ExecutionResult) -> bool:
        return True

    def push(self, result: ExecutionResult) -> None:
        self._service.persist_from_execution_result(result)

    def stop(self) -> None:
        pass
