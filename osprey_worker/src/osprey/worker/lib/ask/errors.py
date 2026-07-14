"""Ask error taxonomy mapped to stable, host-safe codes.

Every error carries a stable ``code`` and a ``public_message`` that is always safe to
show a client -- it never contains a raw exception, provider payload, or credential.
``preflight`` errors are raised before any event is emitted (the transport maps them
to an HTTP status); non-preflight errors occur mid-stream and become a terminal
``error`` event.
"""

from __future__ import annotations

from typing import Dict, Literal, Optional

AskErrorCode = Literal[
    'unavailable_provider',
    'invalid_request',
    'invalid_model',
    'invalid_context',
    'forbidden',
    'provider_error',
    'tool_error',
    'budget_exceeded',
    'lock_unavailable',
    'serialization_error',
    'cancelled',
    'internal',
]


class AskError(Exception):
    """Base class for all Ask errors."""

    code: AskErrorCode = 'internal'
    http_status: int = 500
    preflight: bool = False
    public_message: str = 'An internal error occurred.'

    def __init__(self, public_message: Optional[str] = None) -> None:
        if public_message is not None:
            self.public_message = public_message
        super().__init__(self.public_message)


# --- Pre-flight errors: raised before streaming; transport returns HTTP ---


class InvalidRequest(AskError):
    code: AskErrorCode = 'invalid_request'
    http_status = 400
    preflight = True
    public_message = 'The request was invalid.'


class InvalidModel(AskError):
    code: AskErrorCode = 'invalid_model'
    http_status = 400
    preflight = True
    public_message = 'The requested model is not available.'


class InvalidContext(AskError):
    code: AskErrorCode = 'invalid_context'
    http_status = 400
    preflight = True
    public_message = 'The request context was invalid.'


class Forbidden(AskError):
    code: AskErrorCode = 'forbidden'
    http_status = 403
    preflight = True
    public_message = 'You do not have access to this conversation.'


class UnavailableProvider(AskError):
    code: AskErrorCode = 'unavailable_provider'
    http_status = 503
    preflight = True
    public_message = 'No language model provider is available.'


class LockUnavailable(AskError):
    code: AskErrorCode = 'lock_unavailable'
    http_status = 409
    preflight = True
    public_message = 'Another turn is already in progress for this conversation.'


# --- In-stream errors: surfaced as a terminal ``error`` event ---


class ProviderError(AskError):
    code: AskErrorCode = 'provider_error'
    public_message = 'The language model provider failed to respond.'


class ToolExecutionError(AskError):
    code: AskErrorCode = 'tool_error'
    public_message = 'A tool failed to execute.'


class BudgetExceeded(AskError):
    code: AskErrorCode = 'budget_exceeded'
    public_message = 'The response exceeded the configured limits.'


class Cancelled(AskError):
    code: AskErrorCode = 'cancelled'
    public_message = 'The request was cancelled.'


class SerializationError(AskError):
    code: AskErrorCode = 'serialization_error'
    public_message = 'The response could not be serialized.'


class InternalError(AskError):
    code: AskErrorCode = 'internal'
    public_message = 'An internal error occurred.'


def to_public_payload(err: AskError) -> Dict[str, str]:
    """Return the safe, client-facing ``{code, message}`` for an error."""
    return {'code': err.code, 'message': err.public_message}
