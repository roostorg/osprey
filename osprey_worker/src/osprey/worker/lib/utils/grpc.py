import grpc
from osprey.worker.lib.pigeon.client import RetryPolicy

DEFAULT_RETRYABLE_GRPC_STATUSES: set[grpc.StatusCode] = {
    grpc.StatusCode.UNAVAILABLE,
    grpc.StatusCode.INTERNAL,
    grpc.StatusCode.UNKNOWN,
    grpc.StatusCode.DEADLINE_EXCEEDED,  # timeout
}

DATA_SERVICES_RETRY_POLICY: RetryPolicy | None = RetryPolicy(
    retryable_grpc_status_codes=DEFAULT_RETRYABLE_GRPC_STATUSES,
    max_secondaries_to_retry=2,
)

SAFETY_RECORD_RETRY_POLICY: RetryPolicy | None = RetryPolicy(
    retryable_grpc_status_codes=DEFAULT_RETRYABLE_GRPC_STATUSES,
    max_secondaries_to_retry=1,
)


def is_grpc_error_retryable(exception: BaseException) -> bool:
    """
    Returns True if the exception is a gRPC error with error code in the list retryable code provided.
    Handles non-gRPC false list scenario.
    """
    return isinstance(exception, grpc.RpcError) and exception.code() in DEFAULT_RETRYABLE_GRPC_STATUSES
