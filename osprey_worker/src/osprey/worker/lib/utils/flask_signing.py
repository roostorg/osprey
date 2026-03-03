import base64
import binascii
import functools
import hashlib
import json
from collections.abc import Callable
from typing import Any, TypeVar
from urllib.parse import urlparse

from Crypto.Hash import SHA256
from Crypto.PublicKey import ECC
from Crypto.Signature import DSS
from flask import abort, request
from six.moves.http_client import BAD_REQUEST, METHOD_NOT_ALLOWED
from typing_extensions import Protocol


def _dss_sig_scheme(key: ECC.EccKey) -> DSS.DssSigScheme:
    return DSS.new(key=key, mode='fips-186-3', encoding='der')


class Signer:
    def __init__(self, key_secret: str, key_id_header: str, signature_header: str):
        self._key_id_header = key_id_header
        self._signature_header = signature_header
        key = ECC.import_key(key_secret)
        self._key_id = _get_key_id(key)
        self._dss_sig_scheme = _dss_sig_scheme(key)

    def sign(self, message: bytes) -> dict[str, str | bytes]:
        signature = base64.b64encode(self._dss_sig_scheme.sign(SHA256.new(data=message)))
        return {self._key_id_header: self._key_id, self._signature_header: signature}

    def sign_url(self, url: str, normalize: bool = True) -> dict[str, str | bytes]:
        """
        `normalize` will normalize the URL to look like Flask's Request.full_path.
        When calling `verify_request_path`, `use_full_path` should be `True` if `normalize` is also `True` when signing.
        """
        if normalize:
            parsed = urlparse(url)
            normalized_url = f'{parsed.path}?{parsed.query}'
            return self.sign(normalized_url.encode())

        return self.sign(url.encode())


def get_public_key_id(public_key: str) -> str:
    key = ECC.import_key(public_key)
    return _get_key_id(key)


def _get_key_id(key: ECC.EccKey) -> str:
    der_key = key.public_key().export_key(format='DER')
    # Returns a different type depending on the format, stubs aren't smart enough to know these are bytes
    der_key_bytes = der_key
    digest = hashlib.sha256(der_key_bytes).digest()
    return base64.b64encode(digest).strip(b'\n=').decode()


class PublicKeyGetterFunction(Protocol):
    def __call__(self, key_id: str) -> ECC.EccKey | None: ...


# Would be great to be able to actually assert that this thing can take a `request_data: dict[str, object]`,
# but mypy isn't great at modifying function types.
WithRequestDataFunction = Callable[..., Any]
FuncT = TypeVar('FuncT', bound=Callable[..., Any])


def verify_request_data(
    public_key_getter: PublicKeyGetterFunction,
    key_id_header: str,
    key_signature_header: str,
    allowed_methods: list[str] | None = None,
) -> Callable[[WithRequestDataFunction], Callable[..., Any]]:
    """
    Wraps a view, verifying the request.data against a key signature using the same method as github,
    per: https://developer.github.com/partnerships/token-scanning/

    Injects the `request_data` kwarg into the wrapped function, or aborts if the
    request cannot be successfully verified.
    """

    def deco(func: WithRequestDataFunction) -> Callable[..., Any]:
        @functools.wraps(func)
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            nonlocal allowed_methods
            if allowed_methods is None:
                allowed_methods = ['POST']

            if request.method not in allowed_methods:
                return abort(METHOD_NOT_ALLOWED, 'Can only verify POST requests.')

            _verify_payload(public_key_getter, key_id_header, key_signature_header, request.data)

            try:
                request_data = json.loads(request.data)
            except ValueError:
                abort(BAD_REQUEST, 'Request data is not JSON.')

            return func(*args, request_data=request_data, **kwargs)

        return wrapped

    return deco


def verify_request_path(
    public_key_getter: PublicKeyGetterFunction,
    key_id_header: str,
    key_signature_header: str,
    use_full_path: bool = True,
    allowed_methods: list[str] | None = None,
) -> Callable[[FuncT], FuncT]:
    """
    Wraps a view, verifying the path and querystring parameters against a key signature. This is similar to
    `verify_request_data` except designed for use with GET's, rather than POST/PUT/DELETE etc.

    All arguments are forwarded unchanged into the wrapped function, or aborts if the
    request cannot be successfully verified.

    `use_full_path` determines whether the full path or the url is used. the full path won't contain the
    scheme, host, or port, but will contain the query string. The url will contain the everything.

    NOTE: This authentication relies on attackers not being able to read requests, since otherwise it is vulnerable
    to replay attacks.
    """

    def deco(func: FuncT) -> FuncT:
        @functools.wraps(func)
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            nonlocal allowed_methods
            if allowed_methods is None:
                allowed_methods = ['GET']

            if request.method not in allowed_methods:
                return abort(METHOD_NOT_ALLOWED, 'Can only verify GET requests.')

            if use_full_path:
                _verify_payload(public_key_getter, key_id_header, key_signature_header, request.full_path.encode())
            else:
                _verify_payload(public_key_getter, key_id_header, key_signature_header, request.url.encode())

            return func(*args, **kwargs)

        return wrapped  # type: ignore # Doesn't know that wrapped is effectively just func

    return deco


def _verify_payload(
    public_key_getter: PublicKeyGetterFunction, key_id_header: str, key_signature_header: str, payload: bytes
) -> None:
    key_id = request.headers.get(key_id_header)
    if not key_id:
        abort(BAD_REQUEST, f"Header '{key_id_header}' is required.")

    signature_raw = request.headers.get(key_signature_header)
    if not signature_raw:
        abort(BAD_REQUEST, f"Header '{key_signature_header}' is required.")

    try:
        signature = base64.b64decode(signature_raw)
    except (TypeError, binascii.Error):  # TypeError is raised in py2, binascii.Error in py3
        abort(BAD_REQUEST, f"Header '{key_signature_header}' is not valid base64.")

    ecc_key = public_key_getter(key_id=key_id)
    if not ecc_key:
        abort(
            BAD_REQUEST,
            "Header '{key_name}' refers to key '{key_id}', which is not found.".format(
                key_name=key_signature_header, key_id=key_id
            ),
        )

    payload_hash = SHA256.new(data=payload)
    verifier = _dss_sig_scheme(key=ecc_key)

    try:
        verifier.verify(payload_hash, signature)
    except ValueError:
        abort(BAD_REQUEST, f"Header '{key_signature_header}' does not match the request data.")
