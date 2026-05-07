import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional
from urllib.parse import urlencode

import sentry_sdk
import websocket
from osprey.engine.executor.execution_context import Action
from osprey.worker.lib.instruments import metrics
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.sinks.sink.input_stream import BaseInputStream
from osprey.worker.sinks.utils.acking_contexts import BaseAckingContext, NoopAckingContext
from websocket import WebSocketConnectionClosedException

logger = get_logger()


DEFAULT_ENDPOINT = 'wss://jetstream2.us-west.bsky.network/subscribe'
COLLECTION_NAMES = {
    'app.bsky.feed.post': 'post',
    'app.bsky.feed.like': 'like',
    'app.bsky.feed.repost': 'repost',
    'app.bsky.graph.follow': 'follow',
    'app.bsky.actor.profile': 'profile',
}
DEFAULT_COLLECTIONS = tuple(COLLECTION_NAMES.keys())


class JetStreamInputStream(BaseInputStream[BaseAckingContext[Action]]):
    """Subscribes to Bluesky's ATProto JetStream WebSocket and yields Osprey Actions.

    JetStream is Bluesky's public real-time firehose for ATProto network activity emitted as
    plain JSON (no CBOR/CAR), making it a convenient high-volume source for exercising Osprey
    against real production traffic. See https://docs.bsky.app/blog/jetstream.

    The JetStream JSON event is passed through unchanged as the Action's data dict, so rules
    target JetStream-native paths directly: ``$.did``, ``$.kind``, ``$.commit.operation``,
    ``$.commit.collection``, ``$.commit.record.text``, ``$.identity.handle``, etc. The
    ``action_name`` is ``<operation>_<short>`` for commit events (e.g. ``create_post``,
    ``delete_like``) using the short names in :data:`COLLECTION_NAMES`, or ``'identity'``
    for identity events. Account events and commits for unmapped collections are skipped.
    """

    def __init__(
        self,
        endpoint: Optional[str] = None,
        wanted_collections: Optional[List[str]] = None,
        reconnect_seconds: float = 2.0,
    ):
        super().__init__()
        self._endpoint = endpoint or DEFAULT_ENDPOINT
        self._wanted_collections = list(wanted_collections) if wanted_collections else list(DEFAULT_COLLECTIONS)
        self._reconnect_seconds = reconnect_seconds
        self._last_time_us: Optional[int] = None

    def _build_url(self) -> str:
        params = [('wantedCollections', c) for c in self._wanted_collections]
        if self._last_time_us is not None:
            params.append(('cursor', str(self._last_time_us)))
        return f'{self._endpoint}?{urlencode(params)}'

    def _gen(self) -> Iterator[BaseAckingContext[Action]]:
        while True:
            try:
                url = self._build_url()
                yield from self._stream_one_connection(url)
            except Exception as e:
                logger.exception(f'JetStream stream error; reconnecting in {self._reconnect_seconds}s: {e}')
                sentry_sdk.capture_exception(e)
            time.sleep(self._reconnect_seconds)

    def _stream_one_connection(self, url: str) -> Iterator[BaseAckingContext[Action]]:
        logger.info(f'Connecting to JetStream at {url}')
        ws = websocket.create_connection(url, timeout=30)
        logger.info('JetStream connection established')
        try:
            while True:
                try:
                    raw = ws.recv()
                except WebSocketConnectionClosedException:
                    logger.info('JetStream connection closed; will reconnect')
                    return
                if not raw:
                    continue
                try:
                    event = json.loads(raw)
                    action = _event_to_action(event, action_id=0)
                except Exception:
                    logger.exception('skipping malformed JetStream event')
                    sentry_sdk.capture_exception()
                    continue
                if action is None:
                    continue
                time_us = event.get('time_us')
                if time_us and isinstance(time_us, int) and time_us > 0:
                    self._last_time_us = time_us
                metrics.increment('jetstream_input_stream.events', tags=[f'action_name:{action.action_name}'])
                yield NoopAckingContext(action)
        finally:
            try:
                ws.close()
            except Exception:
                logger.info('ignored error while closing JetStream socket', exc_info=True)


def _event_to_action(event: Dict[str, Any], action_id: int) -> Optional[Action]:
    """Wrap a JetStream event as an Osprey :class:`Action`, or return ``None`` to skip it.

    The event JSON is passed through as-is so rules can read JetStream-native paths
    (``$.did``, ``$.commit.collection``, ``$.commit.record.text``, etc.). For commit
    events ``action_name`` is ``<operation>_<short>`` (e.g. ``create_post``); for
    identity events it is ``'identity'``. Account events, commits for collections not
    in :data:`COLLECTION_NAMES`, and commits with unexpected operations are skipped.
    """
    kind = event.get('kind')
    if kind not in ('commit', 'identity'):
        return None
    time_us = event.get('time_us')
    if not isinstance(time_us, int) or time_us <= 0:
        return None
    if kind == 'commit':
        commit = event.get('commit') or {}
        operation = commit.get('operation')
        short = COLLECTION_NAMES.get(commit.get('collection', ''))
        if short is None or operation not in ('create', 'update', 'delete'):
            return None
        action_name = f'{operation}_{short}'
    else:
        action_name = 'identity'
    return Action(
        action_id=action_id,
        action_name=action_name,
        data=event,
        timestamp=datetime.fromtimestamp(time_us / 1_000_000, tz=timezone.utc),
    )
