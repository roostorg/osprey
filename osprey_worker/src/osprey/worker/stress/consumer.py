"""Stress harness result consumer.

Subscribes to the `osprey.execution_results` Kafka topic and records the
wall-clock receive time per `ActionId` so the reporter can pair every input
event with its output and compute end-to-end latency.

Producer-agnostic: the same consumer works against synthetic load (#324), an
external source such as the JetStream input stream, or any other input source.
The optional `action_id_filter` decides whether we're in closed-loop matching mode
(only count IDs we know about) or open-loop throughput mode (count everything).
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass

from kafka import KafkaConsumer


@dataclass(frozen=True)
class ConsumerConfig:
    bootstrap_servers: list[str]
    topic: str
    group_id: str
    # If set, only record action_ids in this set. None = open-loop, count all.
    action_id_filter: frozenset[int] | None = None
    # Max wall-clock time to keep reading after start() is called.
    max_runtime_seconds: float = 60.0
    # If we've matched every id in action_id_filter, stop early.
    stop_when_filter_complete: bool = True
    client_id: str = 'osprey-stress-consumer'


class Consumer:
    """Consumes execution_results on a background thread.

    `start()` -> `wait()` -> read `consumed`. `stop()` to abort early.
    """

    def __init__(self, config: ConsumerConfig, *, consumer_factory=KafkaConsumer):
        self._config = config
        self._consumer_factory = consumer_factory
        self._consumed: dict[int, float] = {}
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._error: BaseException | None = None

    @property
    def consumed(self) -> dict[int, float]:
        return dict(self._consumed)

    @property
    def error(self) -> BaseException | None:
        return self._error

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError('Consumer already started')
        self._thread = threading.Thread(target=self._run, name='stress-consumer', daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()

    def wait(self, timeout: float | None = None) -> None:
        if self._thread is None:
            return
        self._thread.join(timeout=timeout)

    def _run(self) -> None:
        try:
            consumer = self._consumer_factory(
                self._config.topic,
                bootstrap_servers=self._config.bootstrap_servers,
                group_id=self._config.group_id,
                client_id=self._config.client_id,
                # Start at 'latest' so we don't pick up old results from
                # previous runs. The producer should be started after the
                # consumer is assigned a partition (caller orchestrates).
                auto_offset_reset='latest',
                enable_auto_commit=False,
                # Bounded poll so the stop_event check has a chance to fire.
                consumer_timeout_ms=200,
            )
        except Exception as e:
            self._error = e
            return

        deadline = time.monotonic() + self._config.max_runtime_seconds
        try:
            while not self._stop_event.is_set() and time.monotonic() < deadline:
                # The kafka-python iterator yields until consumer_timeout_ms
                # of inactivity, then raises StopIteration. We catch that and
                # loop so we can re-check stop conditions.
                try:
                    for message in consumer:
                        if self._stop_event.is_set():
                            break
                        recorded = self._record(message.value)
                        if (
                            recorded
                            and self._config.stop_when_filter_complete
                            and self._config.action_id_filter is not None
                            and len(self._consumed) >= len(self._config.action_id_filter)
                        ):
                            return
                except StopIteration:
                    # Idle poll cycle — loop and re-check stop conditions.
                    continue
        except Exception as e:
            self._error = e
        finally:
            try:
                consumer.close(autocommit=False)
            except Exception:
                # Best-effort close during shutdown: the kafka client may already
                # be in a half-disconnected state, and a noisy close() error here
                # would mask the consumed/error state the caller needs to read.
                pass

    def _record(self, raw: bytes) -> bool:
        """Parse one result message; return True if it counted toward consumed."""
        try:
            payload = json.loads(raw.decode('utf-8'))
        except (ValueError, UnicodeDecodeError):
            # Garbage at the byte level is tolerated — Kafka can carry malformed
            # bytes from broken producers and we shouldn't take down the
            # measurement run for transport noise.
            return False
        action_id = payload.get('ActionId')
        if not isinstance(action_id, int):
            # Schema-level violation: a well-formed JSON result missing a valid
            # ActionId means the harness was pointed at rules that didn't wire
            # GetActionId() (or wired it wrong). Raise loudly so the caller sees
            # the misconfiguration via consumer.error instead of getting silent
            # zero matches.
            raise ValueError(
                f'result message has missing or non-int ActionId (got {type(action_id).__name__}); '
                f'ensure the rule set exposes ActionId via GetActionId()'
            )
        if self._config.action_id_filter is not None and action_id not in self._config.action_id_filter:
            return False
        # First-write-wins so duplicate effect dispatches don't reset the time.
        if action_id not in self._consumed:
            self._consumed[action_id] = time.time()
            return True
        return False
