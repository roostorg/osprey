import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional
from urllib.parse import urlencode

import gevent
import websocket
from gevent.queue import Queue
from osprey.engine.executor.execution_context import Action
from osprey.worker.lib.backoff import Backoff
from osprey.worker.lib.instruments import metrics
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.lib.snowflake import generate_snowflake_batch
from osprey.worker.sinks.sink.input_stream import BaseInputStream
from osprey.worker.sinks.utils.acking_contexts import BaseAckingContext, NoopAckingContext

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
SNOWFLAKE_BATCH_SIZE = 250
PING_INTERVAL_SECONDS = 20
PING_TIMEOUT_SECONDS = 10


class JetStreamInputStream(BaseInputStream[BaseAckingContext[Action]]):
    """An Osprey event input stream that subscribes to the ATProto JetStream websocket and yields
    Osprey actions.

    The JetStream JSON event is passed through unchanged as the Action's data dict, so rules may
    target the JetStream-native paths directly, like $.did, $.kind, or $.commit.operation.
    """

    def __init__(
        self,
        endpoint: Optional[str] = None,
        wanted_collections: Optional[List[str]] = None,
        reconnect_seconds: float = 2.0,
        max_reconnect_seconds: float = 60.0,
    ):
        super().__init__()
        self._endpoint = endpoint or DEFAULT_ENDPOINT
        self._wanted_collections = list(wanted_collections) if wanted_collections else list(DEFAULT_COLLECTIONS)
        self._backoff = Backoff(min_delay=reconnect_seconds, max_delay=max_reconnect_seconds)
        self._last_time_us: Optional[int] = None
        self._snowflake_buffer: List[int] = []

    def _next_action_id(self) -> int:
        if not self._snowflake_buffer:
            batch = generate_snowflake_batch(count=SNOWFLAKE_BATCH_SIZE, retries=3)
            self._snowflake_buffer = [s.to_int() for s in batch]
        return self._snowflake_buffer.pop()

    def _build_url(self) -> str:
        params = [('wantedCollections', c) for c in self._wanted_collections]
        if self._last_time_us is not None:
            params.append(('cursor', str(self._last_time_us)))
        return f'{self._endpoint}?{urlencode(params)}'

    def _gen(self) -> Iterator[BaseAckingContext[Action]]:
        while True:
            had_event = False
            try:
                url = self._build_url()
                for ctx in self._stream_one_connection(url):
                    had_event = True
                    yield ctx
            except Exception as e:
                logger.exception(f'JetStream stream error: {e}')

            if had_event:
                self._backoff.succeed()
                delay = self._backoff.current
            else:
                delay = self._backoff.fail()
            logger.info(f'Reconnecting in {delay:.1f}s')
            time.sleep(delay)

    def _stream_one_connection(self, url: str) -> Iterator[BaseAckingContext[Action]]:
        # WebSocketApp drives PING/PONG keepalive on its own greenlet; we bridge its
        # callback API into this generator via a gevent.queue.Queue. The 'done' sentinel
        # is pushed by on_close/on_error, and also as a safety net by greenlet.link in
        # case run_forever exits without firing on_close (e.g., uncaught exception).
        queue: 'Queue[tuple[str, Optional[bytes]]]' = Queue()

        def on_open(ws: Any) -> None:
            logger.info('JetStream connection established')

        def on_message(ws: Any, raw: Any) -> None:
            queue.put(('message', raw))

        def on_close(ws: Any, status: Any, msg: Any) -> None:
            logger.info(f'JetStream connection closed (status={status}); will reconnect')
            queue.put(('done', None))

        def on_error(ws: Any, err: Any) -> None:
            logger.warning(f'JetStream connection error: {err}; will reconnect')
            queue.put(('done', None))

        logger.info(f'Connecting to JetStream at {url}')
        app = websocket.WebSocketApp(
            url,
            on_open=on_open,
            on_message=on_message,
            on_close=on_close,
            on_error=on_error,
        )
        runner = gevent.spawn(app.run_forever, ping_interval=PING_INTERVAL_SECONDS, ping_timeout=PING_TIMEOUT_SECONDS)
        runner.link(lambda _g: queue.put(('done', None)))

        try:
            while True:
                kind, raw = queue.get()
                if kind == 'done':
                    return
                if not raw:
                    continue
                try:
                    event = json.loads(raw)
                except json.JSONDecodeError as e:
                    raw_bytes = raw if isinstance(raw, bytes) else str(raw).encode('utf-8', errors='replace')
                    logger.warning(f'JetStream payload was not valid JSON ({e}); first 200 bytes: {raw_bytes[:200]!r}')
                    continue
                if not isinstance(event, dict):
                    logger.warning(
                        f'JetStream payload parsed to non-object JSON (got {type(event).__name__}); skipping'
                    )
                    continue
                try:
                    action_id = self._next_action_id()
                except Exception:
                    logger.exception('failed to mint action_id from snowflake-id-worker; skipping event')
                    continue
                action = _event_to_action(event, action_id=action_id)
                if action is None:
                    continue
                time_us = event.get('time_us')
                if time_us and isinstance(time_us, int) and time_us > 0:
                    self._last_time_us = time_us
                metrics.increment('jetstream_input_stream.events', tags=[f'action_name:{action.action_name}'])
                yield NoopAckingContext(action)
        finally:
            try:
                app.close()
            except Exception:
                logger.info('ignored error while closing JetStream WebSocketApp', exc_info=True)
            runner.join(timeout=5)


def _event_to_action(event: Dict[str, Any], action_id: int) -> Optional[Action]:
    """Wraps a JetStream event as an Osprey action, or returns None if it should be skipped."""
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
