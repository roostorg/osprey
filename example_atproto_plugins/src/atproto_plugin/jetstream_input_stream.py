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

logger = get_logger()


DEFAULT_ENDPOINT = 'wss://jetstream2.us-west.bsky.network/subscribe'
DEFAULT_COLLECTIONS = (
    'app.bsky.feed.post',
    'app.bsky.feed.like',
    'app.bsky.feed.repost',
    'app.bsky.graph.follow',
)


class JetStreamInputStream(BaseInputStream[BaseAckingContext[Action]]):
    """Subscribes to Bluesky's ATProto JetStream WebSocket and yields Osprey Actions.

    JetStream is Bluesky's public real-time firehose for ATProto network activity emitted as
    plain JSON (no CBOR/CAR), making it a convenient high-volume source for exercising Osprey
    against real production traffic. See https://docs.bsky.app/blog/jetstream.

    Each ATProto commit is mapped to an Osprey :class:`Action` whose data dictionary exposes
    the actor DID, collection, operation, and the raw record. Identity and account events are
    skipped — the sample is concerned with content events.
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

    def _build_url(self) -> str:
        params = [('wantedCollections', c) for c in self._wanted_collections]
        return f'{self._endpoint}?{urlencode(params)}'

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
                    except json.JSONDecodeError:
                        logger.warning(f'Failed to parse JetStream event: {raw[:200]!r}')
                        continue

                    action = _event_to_action(event)
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
                        pass
            time.sleep(self._reconnect_seconds)


def _event_to_action(event: Dict[str, Any]) -> Optional[Action]:
    """Map a JetStream event dict into an Osprey :class:`Action`, or return ``None`` to skip it."""
    if event.get('kind') != 'commit':
        return None
    commit = event.get('commit') or {}
    operation = commit.get('operation')
    collection = commit.get('collection', '') or ''
    did = event.get('did', '') or ''
    time_us = event.get('time_us')
    if not time_us:
        return None

    short = collection.rsplit('.', 1)[-1] if collection else 'unknown'
    action_name = f'{operation}_{short}' if operation else short

    record = commit.get('record') or {}

    data: Dict[str, Any] = {
        'did': did,
        'collection': collection,
        'operation': operation,
        'rkey': commit.get('rkey'),
        'rev': commit.get('rev'),
        'cid': commit.get('cid'),
        'event_type': action_name,
        'record': record,
    }

    return Action(
        action_id=int(time_us),
        action_name=action_name,
        data=data,
        timestamp=datetime.fromtimestamp(time_us / 1_000_000, tz=timezone.utc),
    )
