from __future__ import annotations

from flask import Flask, request
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.ui_api.osprey.lib.users import User

logger = get_logger(__name__)


def set_dummy_claim() -> None:
    set_claims({'email': request.headers.get('X-Test-Email', 'local-dev@localhost')})


def set_claims(claims: dict[object, object]) -> None:
    request.claims = claims  # type: ignore[attr-defined]


def get_current_user_email() -> str:
    claims: dict[object, object] = request.claims  # type: ignore[attr-defined]
    email = claims.get('email')
    assert isinstance(email, str), f'Could not get email from claims {claims!r}'
    return email


def get_current_user() -> User:
    return request.current_user  # type: ignore[attr-defined]


def set_user_on_request() -> None:
    # don't set user on healthcheck
    if request.endpoint == 'health':
        return

    user_email = get_current_user_email()
    request.current_user = User(email=user_email)  # type: ignore[attr-defined]


def init_app(app: Flask) -> None:
    app.before_request(set_dummy_claim)
    app.before_request(set_user_on_request)
