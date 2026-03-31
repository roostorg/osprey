"""Shadow-mode BigQuery output sink for the async worker.

Publishes execution results to the same BQ Pub/Sub topic as the gevent worker,
but tagged with source='osprey-async' so results can be compared side-by-side.
This is the ONLY output sink in shadow mode — no Druid, no SafetyRecord, no effects.

The underlying BigQueryOutputSink and PubSubPublisher are pure Python (no gevent),
so they run fine in an executor thread via SyncOutputSinkAdapter.
"""

import asyncio
import json
import os
from dataclasses import asdict
from typing import Optional, Set

import mmh3
import sentry_sdk
from osprey.engine import shared_constants
from osprey.engine.executor.execution_context import ExecutionResult
from osprey.worker.lib.instruments import metrics
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.lib.publisher import PubSubPublisher
from pydantic import BaseModel

from osprey.async_worker.adaptor.interfaces import AsyncBaseOutputSink

logger = get_logger(__name__)

# Maximum hash value for normalization (2^31) — matches discord_api pattern
MAX_HASH = 2147483648.0

# Default 1% sample rate for BQ ingestion
DEFAULT_SAMPLE_RATE = 0.01


def _should_sample_deterministic(target: int, sample_rate: float) -> bool:
    """Deterministic sampling based on action_id hash."""
    if sample_rate <= 0.0:
        return False
    if sample_rate >= 1.0:
        return True
    value = abs(mmh3.hash(str(target)) / MAX_HASH)
    return value < sample_rate


class AsyncBigQueryShadowSink(AsyncBaseOutputSink):
    """Async output sink that publishes execution results to BQ via Pub/Sub.

    Tagged with source='osprey-async' so results from the async worker can be
    compared against the gevent worker's results (source='osprey') in BigQuery.
    """

    def __init__(
        self,
        project_id: str,
        topic_id: str,
        sample_rate: float = DEFAULT_SAMPLE_RATE,
        allowed_actions: Optional[Set[str]] = None,
    ):
        self._publisher = PubSubPublisher(
            project_id=project_id,
            topic_id=topic_id,
        )
        self._sample_rate = sample_rate
        self._allowed_actions = allowed_actions

    def will_do_work(self, result: ExecutionResult) -> bool:
        if self._allowed_actions is not None and result.action.action_name not in self._allowed_actions:
            return False

        if os.getenv('ENVIRONMENT') not in ['production']:
            return True

        return _should_sample_deterministic(result.action.action_id, self._sample_rate)

    async def push(self, result: ExecutionResult) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._sync_push, result)

    def _sync_push(self, result: ExecutionResult) -> None:
        """Synchronous push — runs in thread pool."""
        try:
            rule_audit_entries = None
            if hasattr(result, 'rule_audit_entries') and result.rule_audit_entries is not None:
                rule_audit_entries = json.dumps([asdict(e) for e in result.rule_audit_entries])

            trace_id = getattr(result, 'trace_id', None)

            # Import the model here to avoid hard dependency on discord_smite at module level.
            # If discord_smite is not installed, this sink can't be used — but the rest of the
            # async worker still works.
            from discord_smite.smite_sinks.sink.output_sink_utils.models import (
                SmiteExecutionResultBigQueryPubsubEvent,
            )

            event = SmiteExecutionResultBigQueryPubsubEvent(
                action_id=str(result.action.action_id),
                action_name=result.action.action_name,
                timestamp=result.action.timestamp,
                error_count=int(len(result.error_infos)),
                sample_rate=int(result.sample_rate),
                classifications=result.extracted_features.get('__classifications', []),
                signals=result.extracted_features.get('__signals', []),
                entity_label_mutations=result.extracted_features.get(
                    shared_constants.ENTITY_LABEL_MUTATION_DIMENSION_NAME, []
                ),
                error_results=result.error_traces_json if result.error_traces_json else None,
                execution_results=result.extracted_features_json,
                source='osprey-async',
                trace_id=trace_id,
                rule_audit_entries=rule_audit_entries,
            )
            self._publisher.publish(event)
            metrics.increment('bigquery_shadow_sink.push.success')
        except Exception as e:
            logger.error(f'Exception in BigQuery shadow sink: {e}')
            metrics.increment(
                'bigquery_shadow_sink.push.error',
                tags=[f'error:{e.__class__.__name__}'],
            )
            sentry_sdk.capture_exception(e)

    async def stop(self) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._publisher.stop)
