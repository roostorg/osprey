"""Optional enrichment UDFs that resolve an ATProto DID to profile fields.

JetStream events identify the actor only by DID, which isn't searchable the way a
handle or display name is. These UDFs resolve a DID to those fields via Bluesky's
public, unauthenticated AppView (`app.bsky.actor.getProfile`).

They are registered by the plugin but wired into rules only via the opt-in
`models/enrichment.sml`, because each unique DID costs an external API call --
great for demos, but a dependency you don't want in a load test. The whole
profile is fetched once per DID and cached, and lookups fail soft (the feature is
simply absent) when the API errors or rate-limits.

See the README's "Extending the enrichment" section for how to expose more of the
profile (account age, follower counts, existing labels) from the same cached fetch.
"""

from collections import OrderedDict
from threading import Lock
from typing import Any

import requests
from osprey.engine.executor.execution_context import ExecutionContext, ExpectedUdfException
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase

_ATPROTO_CATEGORY = 'ATProto'
_GET_PROFILE_URL = 'https://public.api.bsky.app/xrpc/app.bsky.actor.getProfile'
_REQUEST_TIMEOUT_SECONDS = 5
_CACHE_MAX_SIZE = 10_000

_session = requests.Session()
# did -> profile dict (the raw getProfile response).
_profile_cache: 'OrderedDict[str, dict[str, Any]]' = OrderedDict()
_cache_lock = Lock()


def _fetch_profile(did: str) -> dict[str, Any]:
    """Return the getProfile response for a DID, hitting the public API on cache miss.

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
    profile = response.json()
    if not isinstance(profile, dict):
        raise ValueError('getProfile did not return an object')

    with _cache_lock:
        _profile_cache[did] = profile
        _profile_cache.move_to_end(did)
        while len(_profile_cache) > _CACHE_MAX_SIZE:
            _profile_cache.popitem(last=False)
    return profile


def _profile_or_skip(did: str) -> dict[str, Any]:
    """Fetch the cached profile, converting any lookup failure into a soft skip."""
    try:
        return _fetch_profile(did)
    except (requests.RequestException, ValueError):
        raise ExpectedUdfException()


class DidArguments(ArgumentsBase):
    did: str
    """The ATProto DID to resolve (e.g. the actor's `$.did`)."""


class AtprotoHandle(UDFBase[DidArguments, str]):
    """Resolves an ATProto DID to its current handle."""

    category = _ATPROTO_CATEGORY
    execute_async = True

    def execute(self, execution_context: ExecutionContext, arguments: DidArguments) -> str:
        handle = _profile_or_skip(arguments.did).get('handle')
        if not handle:
            raise ExpectedUdfException()
        return handle


class AtprotoDisplayName(UDFBase[DidArguments, str]):
    """Resolves an ATProto DID to its display name."""

    category = _ATPROTO_CATEGORY
    execute_async = True

    def execute(self, execution_context: ExecutionContext, arguments: DidArguments) -> str:
        display_name = _profile_or_skip(arguments.did).get('displayName')
        if not display_name:
            raise ExpectedUdfException()
        return display_name
