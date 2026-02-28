"""Nostr-Kafka Bridge: subscribes to a Nostr relay, wraps events in Osprey Action format,
and publishes to Kafka for the Osprey rules engine."""

import asyncio
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

import websockets
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('nostr-kafka-bridge')

RELAY_URL = os.environ.get('RELAY_URL', 'wss://relay.divine.video')
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'osprey.actions_input')
HEALTH_PORT = int(os.environ.get('HEALTH_PORT', '8080'))

# Counter for generating action IDs (combined with timestamp for uniqueness)
_action_counter = 0

connected = False


# --- Health check ---
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        status = 200 if connected else 503
        self.send_response(status)
        self.end_headers()
        self.wfile.write(b'ok' if connected else b'disconnected')

    def log_message(self, *_):
        pass


def start_health_server():
    server = HTTPServer(('0.0.0.0', HEALTH_PORT), HealthHandler)
    Thread(target=server.serve_forever, daemon=True).start()
    log.info('Health check listening on :%d', HEALTH_PORT)


# --- Kafka producer ---
def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
        value_serializer=lambda v: json.dumps(v).encode(),
    )


def _next_action_id() -> int:
    """Generate a unique action ID from timestamp + counter."""
    global _action_counter
    _action_counter += 1
    # Use lower 20 bits of unix timestamp + counter to stay within safe int range
    ts_part = int(datetime.now(timezone.utc).timestamp()) % (2**20)
    return ts_part * 100000 + (_action_counter % 100000)


def _wrap_nostr_event(event: dict) -> dict:
    """Wrap a raw Nostr event into the Osprey Action envelope format.

    Extracts tag data needed by SML models (reported_event_id, reported_pubkey,
    report_reason, mentioned_pubkeys) so rules can reference them via JsonData paths.
    """
    kind = event.get('kind', 0)
    created_at = event.get('created_at', 0)
    tags = event.get('tags', [])

    # Build the data payload starting with raw Nostr fields
    data = {
        'event_id': event.get('id', ''),
        'pubkey': event.get('pubkey', ''),
        'kind': kind,
        'created_at': created_at,
        'content': event.get('content', ''),
        'tags': tags,
    }

    # Extract mentioned pubkeys from p-tags (for kind 1 notes)
    p_tags = [t[1] for t in tags if isinstance(t, list) and len(t) >= 2 and t[0] == 'p']
    if p_tags:
        data['mentioned_pubkeys'] = p_tags

    # Extract report-specific fields for kind 1984 (NIP-56 moderation reports)
    if kind == 1984:
        e_tags = [t for t in tags if isinstance(t, list) and len(t) >= 2 and t[0] == 'e']
        if e_tags:
            data['reported_event_id'] = e_tags[0][1]
        if p_tags:
            data['reported_pubkey'] = p_tags[0]
        # Report reason from the "report" tag or "l" tag
        for t in tags:
            if isinstance(t, list) and len(t) >= 2:
                if t[0] == 'report':
                    data['report_reason'] = t[1]
                    break
                if t[0] == 'l' and len(t) >= 3 and t[2] == 'MOD':
                    data['report_reason'] = t[1]
                    break
        # Fallback: check content for common report keywords
        if 'report_reason' not in data:
            content_lower = event.get('content', '').lower()
            for reason in ('spam', 'nudity', 'csam', 'impersonation', 'illegal'):
                if reason in content_lower:
                    data['report_reason'] = reason
                    break

    # ISO timestamp for the wrapper
    try:
        send_time = datetime.fromtimestamp(created_at, tz=timezone.utc).isoformat()
    except (OSError, ValueError):
        send_time = datetime.now(timezone.utc).isoformat()

    return {
        'send_time': send_time,
        'data': {
            'action_id': str(_next_action_id()),
            'action_name': f'nostr_kind_{kind}',
            'data': data,
        },
    }


# --- Main loop ---
async def bridge():
    global connected
    producer = make_producer()
    backoff = 1
    event_count = 0

    while True:
        try:
            sub_id = uuid.uuid4().hex[:16]
            log.info('Connecting to %s (sub %s)', RELAY_URL, sub_id)

            async with websockets.connect(RELAY_URL) as ws:
                # Subscribe to all events
                await ws.send(json.dumps(['REQ', sub_id, {}]))
                connected = True
                backoff = 1
                log.info('Connected and subscribed')

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    if not isinstance(msg, list) or len(msg) < 3:
                        continue

                    if msg[0] == 'EVENT':
                        event = msg[2]
                        wrapped = _wrap_nostr_event(event)
                        producer.send(KAFKA_TOPIC, value=wrapped)
                        event_count += 1
                        if event_count % 100 == 1:
                            log.info('Published %d events (latest: kind %s id %s)',
                                     event_count, event.get('kind', '?'),
                                     event.get('id', '?')[:12])
                        else:
                            log.debug('Published event %s', event.get('id', '?')[:12])

        except Exception as exc:
            connected = False
            log.warning('Disconnected (%s), retrying in %ds', exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


def main():
    start_health_server()
    asyncio.run(bridge())


if __name__ == '__main__':
    main()
