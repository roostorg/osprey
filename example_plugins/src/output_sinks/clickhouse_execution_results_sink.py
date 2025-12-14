import logging
from datetime import datetime
from typing import Any, Dict, List, Set

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from gevent.lock import RLock
from osprey.engine.executor.execution_context import ExecutionResult
from osprey.worker.lib.config import Config
from osprey.worker.sinks.sink.output_sink import BaseOutputSink

logger = logging.getLogger('clickhouse_execution_results_sink')

# define a batch size, this should likely be configurable or even adjusted automatically based on
# rate of events
DEFAULT_BATCH_SIZE = 1000


class ClickhouseExecutionResultsSink(BaseOutputSink):
    def __init__(self, config: Config):
        ch_host = config.get_str('OSPREY_CLICKHOUSE_HOST', 'localhost')
        ch_port = config.get_int('OSPREY_CLICKHOUSE_PORT', 8123)
        ch_user = config.get_str('OSPREY_CLICKHOUSE_USER', 'default')
        ch_pass = config.get_str('OSPREY_CLICKHOUSE_PASSWORD', 'clickhouse')
        ch_db = config.get_str('OSPREY_CLICKHOUSE_DB', 'default')
        ch_table = config.get_str('OSPREY_CLICKHOUSE_TABLE', 'osprey_execution_results')

        # batch size needs to be adjusted based on throughput. consider a batch size that is some 3-4x your
        # peak throughput rate, since clickhouse does get upset if you attempt to insert too small of batches
        # at too fast of a rate. note that batch sizes may be considerably large.
        ch_batch_size = config.get_int('OSPREY_CLICKHOUSE_BATCH_SIZE', DEFAULT_BATCH_SIZE)

        self._ch_database = ch_db
        self._ch_table = ch_table
        self._ch_batch_size = ch_batch_size

        self._ch_client: Client = clickhouse_connect.get_client(
            host=ch_host,
            port=ch_port,
            username=ch_user,
            password=ch_pass,
        )

        self._batch: List[Dict[str, Any]] = []
        self._batch_lock = RLock()
        self._schema_lock = RLock()

        self._known_columns: Set[str] = self._get_schema()
        logger.info(f'Initialized Clickhouse sink with {len(self._known_columns)} known columns in the schema')

    def will_do_work(self, result: ExecutionResult) -> bool:
        return True

    def push(self, result: ExecutionResult) -> None:
        """Add result to batch and insert when batch is full"""
        row: Dict[str, Any] = {}
        row.update(result.extracted_features)

        should_flush = False
        with self._batch_lock:
            self._batch.append(row)
            if len(self._batch) >= self._ch_batch_size:
                should_flush = True

        if should_flush:
            self._flush_batch()

    def _flush_batch(self) -> None:
        """Insert the current batch into Clickhouse"""
        with self._batch_lock:
            if not self._batch:
                return

            batch_to_flush = self._batch.copy()
            self._batch.clear()

        # put all the column names into a set
        all_fields: Set[str] = set()
        for row in batch_to_flush:
            all_fields.update(row.keys())

        # determine if there are new columns and if so, try to update the schema
        new_fields = all_fields - self._known_columns
        if new_fields:
            try:
                with self._schema_lock:
                    # check again after acquring lock incase another greenlet handled the update
                    new_fields = all_fields - self._known_columns
                    if new_fields:
                        self._add_columns(new_fields, batch_to_flush)
                        self._known_columns.update(new_fields)
            except Exception as e:
                logger.error(f'Error updating the Clickhouse table schema: {e}')
                # if there's a failure just re-add to the batch
                # TODO: this should be a bit more resilient, i.e. this could infinitely grow in size if
                # errors are persistent...
                with self._batch_lock:
                    self._batch.extend(batch_to_flush)
                raise

        col_names = self._known_columns
        data_rows: List[Any | None] = []
        for row in batch_to_flush:
            data_row = [row.get(col) for col in col_names]
            data_rows.append(data_row)

        try:
            self._ch_client.insert(
                table=f'{self._ch_database}.{self._ch_table}', data=data_rows, column_names=col_names
            )
            logger.info(f'Inserted {len(batch_to_flush)} rows into ClickHouse')
        except Exception as e:
            logger.error(f'Error flushing batch to Clickhouse: {e}')
            # as above, re-add to the batch on failures. same TODO: as above too
            with self._batch_lock:
                self._batch.extend(batch_to_flush)
            raise

    def _get_schema(self) -> Set[str]:
        """Get the current schema of the Clickhouse table"""
        try:
            result = self._ch_client.query(f'DESCRIBE TABLE {self._ch_database}.{self._ch_table}')
            return {row[0] for row in result.result_rows}
        except Exception as e:
            logger.warning(f'Could not get current schema: {e}')
            return set()

    def _add_columns(self, new_fields: Set[str], batch_sample: List[Dict[str, Any]]) -> None:
        """Add new columns to the Clickhouse table"""
        if not new_fields:
            return

        alter_statements: List[str] = []
        for field_name in new_fields:
            column_type = self._infer_column_type(field_name, batch_sample)
            alter_statements.append(f'ADD COLUMN IF NOT EXISTS `{field_name}` {column_type}')

        alter_query = f"""
            ALTER TABLE {self._ch_database}.{self._ch_table}
            {', '.join(alter_statements)}
        """

        try:
            self._ch_client.command(alter_query)
            logger.info(f'Added columns: {", ".join(new_fields)}')
        except Exception as e:
            logger.error(f'Error adding columns {new_fields}: {e}')
            raise

    def _infer_column_type(self, field_name: str, batch_sample: List[Dict[str, Any]]) -> str:
        """Infer Clickhouse column type from sample data"""
        sample_value = None
        for row in batch_sample:
            if field_name in row and row[field_name] is not None:
                sample_value = row[field_name]
                break

        if sample_value is None:
            return 'Nullable(String)'

        if isinstance(sample_value, bool):
            return 'UInt8'
        elif isinstance(sample_value, int):
            return 'Nullable(Int64)'
        elif isinstance(sample_value, float):
            return 'Nullable(Float64)'
        elif isinstance(sample_value, datetime):
            return 'Nullable(DateTime64(3))'
        elif isinstance(sample_value, list):
            return 'Array(String)'
        else:
            return 'Nullable(String)'

    def stop(self) -> None:
        """Flush any remaining data on shutdown"""
        logger.info('Stopping Clickhouse sink, flushing remaining batch...')
        self._flush_batch()
