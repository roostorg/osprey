"""Tests for AsyncKafkaInputStream.

These exercise the decode contract against the Osprey-format envelope that an
upstream producer (e.g. at-kafka in Osprey-compatible mode) writes, plus the
poll/skip/stop behaviour of the async stream, without a real Kafka broker.
"""

import json
from typing import Any, List, Mapping, Sequence

from osprey.async_worker.sinks.sink.input_stream import AsyncKafkaInputStream
from osprey.worker.sinks.utils.acking_contexts_base import NoopAckingContext


class _FakeRecord:
    def __init__(self, value: Any):
        self.value = value


class _FakeConsumer:
    """Returns each queued batch once per poll(), then empty batches."""

    def __init__(self, batches: Sequence[Sequence[_FakeRecord]]):
        self._batches = list(batches)
        self.closed = False

    def poll(self, timeout_ms: int = 0, max_records: int = 0) -> Mapping[str, Sequence[_FakeRecord]]:
        if self._batches:
            return {'tp-0': self._batches.pop(0)}
        return {}

    def close(self) -> None:
        self.closed = True


def _osprey_envelope(action_id: int, action_name: str, payload: dict) -> bytes:
    return json.dumps(
        {
            'send_time': '2024-01-01T12:00:00Z',
            'data': {
                'action_id': action_id,
                'action_name': action_name,
                'data': payload,
                'secret_data': {},
                'encoding': 'UTF8',
            },
        }
    ).encode('utf-8')


def test_decode_maps_osprey_envelope_to_action() -> None:
    value = _osprey_envelope(123, 'operation#create', {'did': 'did:plc:abc', 'message': 'hello'})

    action = AsyncKafkaInputStream.decode(value)

    assert action.action_id == 123
    assert action.action_name == 'operation#create'
    assert action.data == {'did': 'did:plc:abc', 'message': 'hello'}
    # send_time parsed into a tz-aware datetime
    assert action.timestamp.year == 2024


def test_decode_coerces_string_action_id() -> None:
    value = _osprey_envelope('456', 'identity', {})
    # action_id arrives as a JSON string; decode must coerce to int.
    raw = json.loads(value)
    raw['data']['action_id'] = '456'
    action = AsyncKafkaInputStream.decode(json.dumps(raw).encode('utf-8'))
    assert action.action_id == 456


async def test_gen_yields_decoded_actions_then_stops() -> None:
    records = [
        _FakeRecord(_osprey_envelope(1, 'operation#create', {'message': 'a'})),
        _FakeRecord(_osprey_envelope(2, 'operation#create', {'message': 'b'})),
    ]
    consumer = _FakeConsumer([records])
    stream = AsyncKafkaInputStream(consumer, poll_timeout_ms=1, max_records=10)

    collected: List[int] = []
    async for ctx in stream:
        assert isinstance(ctx, NoopAckingContext)
        with ctx as action:
            collected.append(action.action_id)
        if len(collected) == 2:
            await stream.stop()
            break

    assert collected == [1, 2]
    assert consumer.closed is True


async def test_gen_skips_malformed_record_and_continues() -> None:
    records = [
        _FakeRecord(b'not-json'),
        _FakeRecord(_osprey_envelope(7, 'operation#create', {'message': 'ok'})),
    ]
    consumer = _FakeConsumer([records])
    stream = AsyncKafkaInputStream(consumer, poll_timeout_ms=1, max_records=10)

    collected: List[int] = []
    async for ctx in stream:
        with ctx as action:
            collected.append(action.action_id)
        await stream.stop()
        break

    # The malformed record is skipped; the next valid one is yielded.
    assert collected == [7]
