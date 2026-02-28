"""ClickHouse output sink — replaces KafkaOutputSink for Divine's stack.

Writes rule execution results directly to ClickHouse instead of
routing through Kafka → Druid. The table schema mirrors what Druid
would ingest so the query UI works unchanged.
"""

import json
from typing import Any

import sentry_sdk
from osprey.engine.executor.execution_context import ExecutionResult
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.sinks.sink.output_sink import BaseOutputSink

logger = get_logger()

# Default batch size before flushing to ClickHouse
DEFAULT_BATCH_SIZE = 500
DEFAULT_FLUSH_INTERVAL_SECONDS = 5


class ClickHouseOutputSink(BaseOutputSink):
    """An output sink that writes extracted features to a ClickHouse table.

    Uses clickhouse-connect for efficient batch inserts with configurable
    flush interval and batch size.
    """

    timeout: float = 10.0
    max_retries: int = 2

    def __init__(
        self,
        clickhouse_client: Any,  # clickhouse_connect.driver.Client
        table: str = 'osprey_events',
        database: str = 'osprey',
        batch_size: int = DEFAULT_BATCH_SIZE,
    ):
        self._client = clickhouse_client
        self._table = table
        self._database = database
        self._batch_size = batch_size
        self._buffer: list[dict[str, Any]] = []
        logger.info(f'ClickHouseOutputSink initialized: {database}.{table} (batch_size={batch_size})')

    def will_do_work(self, result: ExecutionResult) -> bool:
        return True

    def push(self, result: ExecutionResult) -> None:
        try:
            features = json.loads(result.extracted_features_json)

            row: dict[str, Any] = {
                '__time': result.action.timestamp.isoformat(),
                '__action_id': result.action.action_id,
            }

            # Add features, skipping internal __ fields already handled above
            for key, val in features.items():
                if key.startswith('__'):
                    continue
                if isinstance(val, (list, dict)):
                    row[key] = json.dumps(val)
                elif val is None:
                    row[key] = ''
                else:
                    row[key] = val

            # Persist entity label mutations from features
            label_mutations = features.get('__entity_label_mutations')
            if label_mutations:
                row['__entity_label_mutations'] = json.dumps(label_mutations) if isinstance(label_mutations, list) else str(label_mutations)

            # Add verdict info if present
            if result.verdicts:
                row['__verdicts'] = json.dumps([v.value if hasattr(v, 'value') else str(v) for v in result.verdicts])

            # Add rule hit info from validator_results
            if result.validator_results:
                row['__rule_hits'] = json.dumps(
                    {str(getattr(name, '__name__', name)): bool(val)
                     for name, val in result.validator_results.items() if val is not None}
                )

            self._buffer.append(row)

            if len(self._buffer) >= self._batch_size:
                self._flush()

        except Exception as e:
            logger.error(f'ClickHouse sink error: {e}')
            sentry_sdk.capture_exception(error=e)

    def _flush(self) -> None:
        if not self._buffer:
            return

        rows_to_flush = len(self._buffer)
        try:
            # clickhouse-connect requires column_names + list-of-lists, not dicts
            column_names = list(self._buffer[0].keys())
            data = [list(row.get(col, '') for col in column_names) for row in self._buffer]
            self._client.insert(
                f'{self._database}.{self._table}',
                data=data,
                column_names=column_names,
                column_oriented=False,
            )
            logger.info(f'Flushed {rows_to_flush} rows to ClickHouse')
        except Exception as e:
            logger.error(f'ClickHouse flush error ({rows_to_flush} rows): {e}')
            sentry_sdk.capture_exception(error=e)
        finally:
            self._buffer.clear()

    def stop(self) -> None:
        self._flush()
