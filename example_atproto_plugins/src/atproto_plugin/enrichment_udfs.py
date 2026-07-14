"""Optional enrichment UDFs that resolve an ATProto DID to profile fields.

JetStream events identify the actor only by DID, which isn't searchable the way a
handle or display name is. These UDFs resolve a DID to those fields via Bluesky's
public, unauthenticated AppView (`app.bsky.actor.getProfile`).

They are registered by the plugin but wired into rules only via the opt-in
`models/enrichment.sml`, because each unique DID costs an external API call --
great for demos, but a dependency you don't want in a load test. The whole
profile is fetched once per DID and cached, and lookups fail soft (the feature is
simply absent) when the API errors or rate-limits.

Async UDFs run concurrently in a gevent pool, so a rule that reads both the handle
and the display name would fire `AtprotoHandle` and `AtprotoDisplayName` at the
same time. Both would miss a cold cache and each make its own `getProfile` call.
To avoid that, a fetch in progress for a DID is shared: the second greenlet waits
on the first one's result instead of making a duplicate request. Cached entries
expire after `_CACHE_TTL_SECONDS`, since handles and display names change; the
fuller approach is to bust a DID's entry when an identity or profile-update event
comes through JetStream, which is left out here to keep the example focused.

See the README's "Extending the enrichment" section for how to expose more of the
profile (account age, follower counts, existing labels) from the same cached fetch.
"""

import time
from collections import OrderedDict
from threading import Event, Lock
from typing import Any

import requests
from osprey.engine.executor.execution_context import ExecutionContext, ExpectedUdfException
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase

_ATPROTO_CATEGORY = 'ATProto'
_GET_PROFILE_URL = 'https://public.api.bsky.app/xrpc/app.bsky.actor.getProfile'
_REQUEST_TIMEOUT_SECONDS = 5
_CACHE_MAX_SIZE = 100_000
_CACHE_TTL_SECONDS = 60 * 60

_session = requests.Session()
# did -> (profile dict, monotonic time at which the entry expires). Ordered so the
# least-recently-used entry is evicted first once the cache is full.
_profile_cache: 'OrderedDict[str, tuple[dict[str, Any], float]]' = OrderedDict()
# did -> a fetch currently in progress, so concurrent misses for the same DID
# (e.g. AtprotoHandle and AtprotoDisplayName on one event) share one API call.
_inflight: dict[str, '_InflightFetch'] = {}
_cache_lock = Lock()


class _InflightFetch:
    """A single `getProfile` call in progress, shared by every greenlet awaiting it.

    The greenlet that created it does the fetch and populates `profile` or `error`
    before setting `done`; waiters block on `done`, then read the result. Under
    gevent's cooperative scheduling this needs no memory barrier -- the waiter only
    runs after `done.set()` yields back to it.
    """

    __slots__ = ('done', 'profile', 'error')

    def __init__(self) -> None:
        self.done = Event()
        self.profile: dict[str, Any] | None = None
        self.error: Exception | None = None


def _get_profile_from_api(did: str) -> dict[str, Any]:
    """Hit the public getProfile endpoint. Raises on any transport/HTTP/parse error."""
    response = _session.get(_GET_PROFILE_URL, params={'actor': did}, timeout=_REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    profile = response.json()
    if not isinstance(profile, dict):
        raise ValueError('getProfile did not return an object')
    return profile


def _fetch_profile(did: str) -> dict[str, Any]:
    """Return the getProfile response for a DID, coalescing concurrent cache misses.

    Raises on any transport/HTTP/parse error so callers can fail soft.
    """
    with _cache_lock:
        cached = _profile_cache.get(did)
        if cached is not None:
            profile, expires_at = cached
            if time.monotonic() < expires_at:
                _profile_cache.move_to_end(did)
                return profile
            del _profile_cache[did]

        inflight = _inflight.get(did)
        is_leader = inflight is None
        if inflight is None:
            inflight = _InflightFetch()
            _inflight[did] = inflight

    if not is_leader:
        # Someone else is already fetching this DID; wait for their result.
        inflight.done.wait()
        if inflight.error is not None:
            raise inflight.error
        assert inflight.profile is not None
        return inflight.profile

    # Leader: do the request outside the lock so a slow call doesn't block other DIDs.
    try:
        profile = _get_profile_from_api(did)
    except Exception as exc:
        inflight.error = exc
        with _cache_lock:
            _inflight.pop(did, None)
        inflight.done.set()
        raise

    with _cache_lock:
        _profile_cache[did] = (profile, time.monotonic() + _CACHE_TTL_SECONDS)
        _profile_cache.move_to_end(did)
        while len(_profile_cache) > _CACHE_MAX_SIZE:
            _profile_cache.popitem(last=False)
        _inflight.pop(did, None)
    inflight.profile = profile
    inflight.done.set()
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
