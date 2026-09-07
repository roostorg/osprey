from __future__ import annotations

from flask import Flask, request
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.lib.singletons import CONFIG
from osprey.worker.ui_api.osprey.lib.users import User

logger = get_logger(__name__)


#: Who a request is from when nothing says otherwise. Every test fixture grants its
#: abilities to this address, so changing the constant breaks the whole suite -- set
#: `OSPREY_DEV_USER_EMAIL` instead.
DEFAULT_DEV_USER_EMAIL = 'local-dev@localhost'


def set_dummy_claim() -> None:
    """Attach an identity to the request. There is no authentication here.

    Two ways to be someone else, in order of precedence:

    - `X-Test-Email` on the request, which changes the caller for that one request. Good
      for driving a second role from curl while a browser stays signed in as the first.
    - `OSPREY_DEV_USER_EMAIL`, which changes the default for the whole process. Good for
      booting the stack *as* a particular user, e.g. to see the UI an author without
      `CAN_DEPLOY_RULES` actually gets.

    Both exist so nobody has to edit this function to switch user. Editing it changes
    the default the test suite depends on, and every fixture grants abilities to
    `DEFAULT_DEV_USER_EMAIL` -- so a local edit here fails ~78 tests that have nothing
    to do with auth, which is a genuinely confusing hour.

    Note this trusts a client-supplied header: identity here is asserted, not verified.
    Abilities are enforced against whatever the caller claims to be.
    """
    default_email = CONFIG.instance().get_str('OSPREY_DEV_USER_EMAIL', DEFAULT_DEV_USER_EMAIL)
    set_claims({'email': request.headers.get('X-Test-Email', default_email)})


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
