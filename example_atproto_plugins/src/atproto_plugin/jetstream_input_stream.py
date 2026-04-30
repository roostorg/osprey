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
from osprey.worker.lib.snowflake import generate_snowflake_batch
from osprey.worker.sinks.sink.input_stream import BaseInputStream
from osprey.worker.sinks.utils.acking_contexts import BaseAckingContext, NoopAckingContext

logger = get_logger()


DEFAULT_ENDPOINT = 'wss://jetstream2.us-west.bsky.network/subscribe'
DEFAULT_COLLECTIONS = (
    'app.bsky.feed.post',
    'app.bsky.feed.like',
    'app.bsky.feed.repost',
    'app.bsky.graph.follow',
    'app.bsky.actor.profile',
)
SNOWFLAKE_BATCH_SIZE = 250


class JetStreamInputStream(BaseInputStream[BaseAckingContext[Action]]):
    """Subscribes to Bluesky's ATProto JetStream WebSocket and yields Osprey Actions.

    JetStream is Bluesky's public real-time firehose for ATProto network activity emitted as
    plain JSON (no CBOR/CAR), making it a convenient high-volume source for exercising Osprey
    against real production traffic. See https://docs.bsky.app/blog/jetstream.

    The Action shape this stream emits is intended to be source-compatible with
    https://github.com/haileyok/atproto-ruleset, so rules written against that ruleset can be
    pointed at this input stream without translation. Identity events become
    ``action_name='identity'``; commit events become ``operation#<create|update|delete>``.
    Account events are skipped — the sample is concerned with content events.
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
        self._snowflake_buffer: List[int] = []

    def _build_url(self) -> str:
        params = [('wantedCollections', c) for c in self._wanted_collections]
        return f'{self._endpoint}?{urlencode(params)}'

    def _next_action_id(self) -> int:
        if not self._snowflake_buffer:
            batch = generate_snowflake_batch(count=SNOWFLAKE_BATCH_SIZE, retries=3)
            self._snowflake_buffer = [s.to_int() for s in batch]
        return self._snowflake_buffer.pop()

    def _gen(self) -> Iterator[BaseAckingContext[Action]]:
        url = self._build_url()
        while True:
            ws: Optional[websocket.WebSocket] = None
            try:
                logger.info(f'Connecting to JetStream at {url}')
                ws = websocket.create_connection(url)
                logger.info('JetStream connection established')
                while True:
                    raw = ws.recv()
                    if not raw:
                        continue
                    try:
                        event = json.loads(raw)
                        action = _event_to_action(event, action_id=self._next_action_id())
                    except Exception:
                        logger.exception('skipping malformed JetStream event')
                        sentry_sdk.capture_exception()
                        continue
                    if action is None:
                        continue
                    metrics.increment('jetstream_input_stream.events', tags=[f'action_name:{action.action_name}'])
                    yield NoopAckingContext(action)
            except Exception as e:
                logger.exception(f'JetStream stream error; reconnecting in {self._reconnect_seconds}s: {e}')
                sentry_sdk.capture_exception(e)
            finally:
                if ws is not None:
                    try:
                        ws.close()
                    except Exception:
                        logger.debug('ignored error while closing JetStream socket', exc_info=True)
            time.sleep(self._reconnect_seconds)


def _event_to_action(event: Dict[str, Any], action_id: int) -> Optional[Action]:
    """Map a JetStream event dict into an Osprey :class:`Action`, or return ``None`` to skip it.

    Identity events become ``action_name='identity'``; commit events become
    ``operation#<create|update|delete>`` with the body under ``$.operation.*``. Account events
    and any other event kind are skipped.
    """
    time_us = event.get('time_us')
    if time_us is None:
        return None
    timestamp = datetime.fromtimestamp(time_us / 1_000_000, tz=timezone.utc)
    did = event.get('did') or ''

    kind = event.get('kind')
    if kind == 'identity':
        identity = event.get('identity') or {}
        return Action(
            action_id=action_id,
            action_name='identity',
            data={
                'did': did,
                'identity': {'handle': identity.get('handle')},
                'eventMetadata': {},
            },
            timestamp=timestamp,
        )

    if kind == 'commit':
        commit = event.get('commit') or {}
        operation = commit.get('operation')
        collection = commit.get('collection') or ''
        rkey = commit.get('rkey') or ''
        return Action(
            action_id=action_id,
            action_name=f'operation#{operation}' if operation else 'operation',
            data={
                'did': did,
                'operation': {
                    'action': operation,
                    'collection': collection,
                    'path': f'{collection}/{rkey}' if collection and rkey else '',
                    'cid': commit.get('cid'),
                    'record': commit.get('record') or {},
                },
                'eventMetadata': {},
            },
            timestamp=timestamp,
        )

    return None
