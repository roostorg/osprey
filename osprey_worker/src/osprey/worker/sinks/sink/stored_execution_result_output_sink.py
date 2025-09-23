from osprey.engine.executor.execution_context import ExecutionResult
from osprey.worker.lib.storage.stored_execution_result import StoredExecutionResult
from osprey.worker.sinks.sink.output_sink import BaseOutputSink


class StoredExecutionResultOutputSink(BaseOutputSink):
    """An output sink that persists the execution result to an EventRecord."""

    def will_do_work(self, result: ExecutionResult) -> bool:
        return True

    def push(self, result: ExecutionResult) -> None:
        StoredExecutionResult.persist_from_execution_result(result)

    def stop(self) -> None:
        pass
