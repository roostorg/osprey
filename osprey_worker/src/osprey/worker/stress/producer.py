"""Stress harness synthetic producer.

Emits N well-formed Osprey actions to the Kafka input topic at a configurable
rate, recording the wall-clock send time per `action_id` so the reporter can
compute end-to-end latency.

Events match the shape expected by `example_rules/` (the rule set the
test_runner is configured with). The consumer matches each input event to its
output `ExecutionResult` via the `ActionId` extracted feature, which is
surfaced by the `GetActionId()` stdlib UDF.
"""

from __future__ import annotations

import json
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Optional

from kafka import KafkaProducer

# action_ids in this range sit above what the bundled `generate_test_data.sh`
# emits but below 2**53, so they survive JSON's integer-as-float precision.
# A per-run multiplier keyed on the low bits of the run uuid keeps concurrent
# stress runs from colliding with each other.
_ACTION_ID_BASE = 1_000_000_000_000
_RUN_BUCKET = 10_000_000


def _action_id_for(run_id: str, n: int) -> int:
    """Deterministic, per-run-unique integer action_id.

    The id encodes the run_id (so we can filter on the consumer side) and the
    event sequence number, all without exceeding JS safe-integer precision.
    """
    bucket = int(run_id[:6], 16) % _RUN_BUCKET
    return _ACTION_ID_BASE + bucket * _RUN_BUCKET + n


@dataclass(frozen=True)
class ProducerConfig:
    bootstrap_servers: list[str]
    topic: str
    events: int
    rate_per_second: float
    run_id: str
    client_id: str = 'osprey-stress'

    @staticmethod
    def make_run_id() -> str:
        return uuid.uuid4().hex[:8]


def build_event(run_id: str, n: int, *, now: Optional[float] = None) -> tuple[int, bytes]:
    """Return `(action_id, json_bytes)` for the n-th event in a run."""
    action_id = _action_id_for(run_id, n)
    ts = now if now is not None else time.time()
    send_time = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(ts))
    ip_octet = (n % 254) + 1
    payload = {
        'send_time': send_time,
        'data': {
            'action_id': action_id,
            'action_name': 'create_post',
            'data': {
                'user_id': f'stress_user_{n % 100}',
                'ip_address': f'192.168.1.{ip_octet}',
                'event_type': 'create_post',
                'post': {'text': f'hello stress {n}'},
            },
        },
    }
    return action_id, json.dumps(payload).encode('utf-8')


class Producer:
    """Produces events on a background thread.

    Use `start()` to kick off, `wait()` to block until done (or limit reached),
    and `produced` to retrieve the `{action_id: timestamp}` map after stopping.
    `stop()` requests early shutdown — useful for Ctrl+C.
    """

    def __init__(self, config: ProducerConfig, *, producer_factory=KafkaProducer):
        self._config = config
        self._producer_factory = producer_factory
        self._produced: dict[int, float] = {}
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._error: Optional[BaseException] = None

    @property
    def produced(self) -> dict[int, float]:
        return dict(self._produced)

    @property
    def error(self) -> Optional[BaseException]:
        return self._error

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError('Producer already started')
        self._thread = threading.Thread(target=self._run, name='stress-producer', daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()

    def wait(self, timeout: Optional[float] = None) -> None:
        if self._thread is None:
            return
        self._thread.join(timeout=timeout)

    def _run(self) -> None:
        try:
            producer = self._producer_factory(
                bootstrap_servers=self._config.bootstrap_servers,
                client_id=self._config.client_id,
                linger_ms=0,
            )
        except Exception as e:
            self._error = e
            return

        interval = 1.0 / self._config.rate_per_second if self._config.rate_per_second > 0 else 0.0
        next_send = time.monotonic()

        try:
            for n in range(self._config.events):
                if self._stop_event.is_set():
                    break
                action_id, value = build_event(self._config.run_id, n)
                send_at = time.time()
                producer.send(self._config.topic, value=value)
                self._produced[action_id] = send_at

                # Sleep until the next scheduled slot. If we've drifted (Kafka
                # slow / GC pause), reset the schedule to now rather than burst-
                # producing to "catch up."
                next_send += interval
                now = time.monotonic()
                if next_send > now:
                    time.sleep(next_send - now)
                else:
                    next_send = now
        except Exception as e:
            self._error = e
        finally:
            try:
                producer.flush(timeout=10)
                producer.close(timeout=10)
            except Exception as cleanup_error:
                # Don't let a teardown failure mask the original error from
                # the send loop above (which is what the caller actually
                # needs to see); only surface this if nothing else failed.
                if self._error is None:
                    self._error = cleanup_error
