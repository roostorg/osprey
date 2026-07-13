"""Enrichment UDFs that resolve an ATProto DID to human-readable profile fields.

JetStream events only carry the actor's DID (a stable but opaque identifier), not
their handle or display name. These UDFs resolve a DID to those fields via
Bluesky's public, unauthenticated AppView (`app.bsky.actor.getProfile`) so demo
rules and the UI can search on usernames instead of raw DIDs.

Results are cached per-DID so the firehose does not re-fetch the same account on
every event. This is sample-quality enrichment: at full firehose volume the
public API will rate-limit, in which case the UDF fails soft (the feature is
simply absent) rather than erroring. For a smoother demo, narrow
`OSPREY_JETSTREAM_WANTED_COLLECTIONS` to reduce the unique-DID rate.
"""

from collections import OrderedDict
from threading import Lock

import requests
from osprey.engine.executor.execution_context import ExecutionContext, ExpectedUdfException
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase

_ATPROTO_CATEGORY = 'ATProto'
_GET_PROFILE_URL = 'https://public.api.bsky.app/xrpc/app.bsky.actor.getProfile'
_REQUEST_TIMEOUT_SECONDS = 5
_CACHE_MAX_SIZE = 10_000

_session = requests.Session()
# did -> (handle, display_name); either field may be None if the profile omits it.
_profile_cache: 'OrderedDict[str, tuple[str | None, str | None]]' = OrderedDict()
_cache_lock = Lock()


def _resolve_profile(did: str) -> tuple[str | None, str | None]:
    """Return (handle, display_name) for a DID, hitting the public API on cache miss.

    Raises on any transport/HTTP/parse error so callers can fail soft.
    """
    with _cache_lock:
        cached = _profile_cache.get(did)
        if cached is not None:
            _profile_cache.move_to_end(did)
            return cached

    # Fetch outside the lock so a slow request does not block other greenlets.
    response = _session.get(_GET_PROFILE_URL, params={'actor': did}, timeout=_REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    data = response.json()
    result = (data.get('handle'), data.get('displayName'))

    with _cache_lock:
        _profile_cache[did] = result
        _profile_cache.move_to_end(did)
        while len(_profile_cache) > _CACHE_MAX_SIZE:
            _profile_cache.popitem(last=False)
    return result


class DidArguments(ArgumentsBase):
    did: str
    """The ATProto DID to resolve (e.g. the actor's `$.did`)."""


class AtprotoHandle(UDFBase[DidArguments, str]):
    """Resolves an ATProto DID to its current handle via the Bluesky public API."""

    category = _ATPROTO_CATEGORY
    execute_async = True

    def execute(self, execution_context: ExecutionContext, arguments: DidArguments) -> str:
        try:
            handle, _ = _resolve_profile(arguments.did)
        except (requests.RequestException, ValueError):
            raise ExpectedUdfException()
        if not handle:
            raise ExpectedUdfException()
        return handle


class AtprotoDisplayName(UDFBase[DidArguments, str]):
    """Resolves an ATProto DID to its display name via the Bluesky public API."""

    category = _ATPROTO_CATEGORY
    execute_async = True

    def execute(self, execution_context: ExecutionContext, arguments: DidArguments) -> str:
        try:
            _, display_name = _resolve_profile(arguments.did)
        except (requests.RequestException, ValueError):
            raise ExpectedUdfException()
        if not display_name:
            raise ExpectedUdfException()
        return display_name
