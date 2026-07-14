"""Error taxonomy: stable codes/statuses, preflight flags, and safe public payloads."""

from osprey.worker.lib.ask.errors import (
    AskError,
    BudgetExceeded,
    Cancelled,
    Forbidden,
    InternalError,
    InvalidContext,
    InvalidModel,
    InvalidRequest,
    LockUnavailable,
    ProviderError,
    SerializationError,
    ToolExecutionError,
    UnavailableProvider,
    to_public_payload,
)

PREFLIGHT = [
    (InvalidRequest, 'invalid_request', 400),
    (InvalidModel, 'invalid_model', 400),
    (InvalidContext, 'invalid_context', 400),
    (Forbidden, 'forbidden', 403),
    (UnavailableProvider, 'unavailable_provider', 503),
    (LockUnavailable, 'lock_unavailable', 409),
]

IN_STREAM = [
    (ProviderError, 'provider_error'),
    (ToolExecutionError, 'tool_error'),
    (BudgetExceeded, 'budget_exceeded'),
    (Cancelled, 'cancelled'),
    (SerializationError, 'serialization_error'),
    (InternalError, 'internal'),
]


def test_preflight_errors_have_code_status_and_flag():
    for cls, code, status in PREFLIGHT:
        err = cls()
        assert isinstance(err, AskError)
        assert err.code == code
        assert err.http_status == status
        assert err.preflight is True


def test_in_stream_errors_are_not_preflight():
    for cls, code in IN_STREAM:
        err = cls()
        assert err.code == code
        assert err.preflight is False


def test_public_payload_is_safe_and_hides_wrapped_exception():
    secret = 'sk-DEADBEEF top secret'
    try:
        raise ValueError(secret)
    except ValueError as exc:
        err = ProviderError()
        err.__cause__ = exc
    payload = to_public_payload(err)
    assert set(payload) == {'code', 'message'}
    assert payload['code'] == 'provider_error'
    assert secret not in payload['message']
    assert 'ValueError' not in payload['message']
