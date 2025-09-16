from __future__ import annotations

import gzip
import json
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence

import gevent
import google.cloud.storage as storage
import pytz
from google.api_core import retry
from google.cloud.bigtable import row_filters
from google.cloud.bigtable.row import Row
from osprey.engine.executor.execution_context import ExecutionResult
from osprey.worker.lib.instruments import metrics
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.lib.storage.bigtable import osprey_bigtable
from pydantic.main import BaseModel

logger = get_logger()


if TYPE_CHECKING:
    from osprey.worker.ui_api.osprey.lib.abilities import DataCensorAbility

SCYLLA_CONCURRENCY_LIMIT = 100
BIGTABLE_CONCURRENCY_LIMIT = 100
GCS_CONCURRENCY_LIMIT = 100


class ErrorTrace(BaseModel):
    rules_source_location: str
    traceback: str


class StoredExecutionResult(BaseModel):
    """
    Stores the execution result in GCS and BigTable, reads result from GCS with BigTable fallback.
    """

    # NOTE: These fields must match the database column names exactly.
    id: int
    extracted_features: Dict[str, Any]
    error_traces: Sequence[ErrorTrace]
    timestamp: datetime
    action_data: Optional[Dict[str, Any]] = None

    @classmethod
    def persist_from_execution_result(cls, execution_result: ExecutionResult) -> None:
        """Persist execution result to GCS and BigTable."""
        StoredExecutionResultGCS.insert(
            action_id=execution_result.action.action_id,
            extracted_features_json=execution_result.extracted_features_json,
            error_traces_json=execution_result.error_traces_json,
            action_data_json=execution_result.action.data_json,
            timestamp=execution_result.action.timestamp,
        )
        StoredExecutionResultBigTable.insert(
            action_id=execution_result.action.action_id,
            extracted_features_json=execution_result.extracted_features_json,
            error_traces_json=execution_result.error_traces_json,
            action_data_json=execution_result.action.data_json,
            timestamp=execution_result.action.timestamp,
        )

    @classmethod
    def get_one_with_action_data(
        cls, event_record_id: int, data_censor_abilities: Sequence[Optional[DataCensorAbility[Any, Any]]] = ()
    ) -> Optional['StoredExecutionResult']:
        """Get execution result from GCS, fallback to BigTable if not found."""
        gcs_event_record = StoredExecutionResultGCS.select_one(event_record_id)
        if gcs_event_record:
            return StoredExecutionResult.parse_from_query_result(gcs_event_record, data_censor_abilities)

        bigtable_event_record = StoredExecutionResultBigTable.select_one(event_record_id)
        if not bigtable_event_record:
            return None
        return StoredExecutionResult.parse_from_query_result(bigtable_event_record, data_censor_abilities)

    @classmethod
    def get_many(
        cls, action_ids: List[int], data_censor_abilities: Sequence[Optional[DataCensorAbility[Any, Any]]] = ()
    ) -> List['StoredExecutionResult']:
        gcs_results = StoredExecutionResultGCS.select_many(action_ids)
        found_action_ids = {result['id'] for result in gcs_results}

        missing_action_ids = [aid for aid in action_ids if aid not in found_action_ids]
        bigtable_results = []
        if missing_action_ids:
            bigtable_results = StoredExecutionResultBigTable.select_many(missing_action_ids)

        combined_results = gcs_results + bigtable_results
        return sorted(
            [
                StoredExecutionResult.parse_from_query_result(result, data_censor_abilities)
                for result in combined_results
            ],
            key=lambda r: pytz.utc.localize(r.timestamp) if r.timestamp.tzinfo is None else r.timestamp,
            reverse=True,
        )

    @classmethod
    def parse_from_query_result(
        cls, result: Dict[str, Any], data_censor_abilities: Sequence[Optional[DataCensorAbility[Any, Any]]]
    ) -> 'StoredExecutionResult':
        # Apply the data censors
        from osprey.worker.ui_api.osprey.lib.abilities import (
            CanViewActionData,
            CanViewFeatureData,
            DataCensorAbility,
        )

        def _censor_data(
            data: Dict[str, Any],
            field: str,
            data_censor_abilities: List[DataCensorAbility[Any, Any]],
            action_name: str,
        ) -> Optional[Dict[str, Any]]:
            data_at_field = data.get(field)
            if not data_at_field:
                return None
            data_copy: Dict[str, Any] = json.loads(data_at_field)
            if not data_censor_abilities:
                return DataCensorAbility.censor_all_leafs(data_copy)
            for censor in data_censor_abilities:
                censor.censor_data(data_copy, action_name)
            assert isinstance(data_copy, dict)
            return data_copy

        action_name: Optional[str] = None
        extracted_features: Optional[Any] = result.get('extracted_features')
        if extracted_features:
            action_name = json.loads(extracted_features).get('ActionName')
        assert action_name is not None, f'Action name could not be parsed from query result: {str(result)}'

        action_data_censors: List[DataCensorAbility[Any, Any]] = [
            censor for censor in data_censor_abilities if censor and isinstance(censor, CanViewActionData)
        ]
        feature_data_censors: List[DataCensorAbility[Any, Any]] = [
            censor for censor in data_censor_abilities if censor and isinstance(censor, CanViewFeatureData)
        ]
        censored_action_data = _censor_data(result, 'action_data', action_data_censors, action_name)
        censored_feature_data = _censor_data(result, 'extracted_features', feature_data_censors, action_name)
        # Continue as normally
        raw_error_traces = result.get('error_traces')
        if raw_error_traces is None:
            error_traces = []
        else:
            error_traces = json.loads(raw_error_traces)

        assert censored_feature_data is not None
        return cls.construct(
            id=result['id'],
            extracted_features=censored_feature_data,
            error_traces=error_traces,
            timestamp=result['timestamp'],
            action_data=censored_action_data,
        )


# TODO: Add tests
class StoredExecutionResultBigTable:
    retry_policy = retry.Retry(initial=1.0, maximum=2.0, multiplier=1.25, deadline=120.0)

    @classmethod
    def select_one(cls, action_id: int) -> Optional[Dict[str, Any]]:
        row = osprey_bigtable.table('stored_execution_result').read_row(
            cls._encode_action_id(action_id), row_filters.CellsColumnLimitFilter(1)
        )
        if not row:
            return None

        return cls._execution_result_dict_from_row(row)

    # TODO: Add `select_*_minimal` methods

    @classmethod
    def select_many(cls, action_ids: List[int]) -> List[Dict[str, Any]]:
        return [
            row
            for row in gevent.pool.Pool(BIGTABLE_CONCURRENCY_LIMIT).imap(cls.select_one, action_ids)
            if row is not None
        ]

    @classmethod
    def insert(
        cls,
        action_id: int,
        extracted_features_json: str,
        error_traces_json: str,
        timestamp: datetime,
        action_data_json: str,
    ) -> None:
        row = osprey_bigtable.table('stored_execution_result').row(cls._encode_action_id(action_id))
        row.set_cell('execution_result', b'extracted_features', extracted_features_json.encode(), timestamp=timestamp)
        row.set_cell('execution_result', b'error_traces', error_traces_json.encode(), timestamp=timestamp)
        row.set_cell('execution_result', b'timestamp', timestamp.isoformat().encode(), timestamp=timestamp)
        row.set_cell('execution_result', b'action_data', action_data_json.encode(), timestamp=timestamp)
        osprey_bigtable.table('stored_execution_result').mutate_rows([row], retry=cls.retry_policy)

    @classmethod
    def _encode_action_id(cls, action_id_snowflake: int) -> bytes:
        """Constructs a bigtable key for a given snowflake."""
        timestamp_portion = action_id_snowflake >> 22
        # reverse the last 4 characters of the timestamp to create a
        # uniformly distributed prefix space.
        key_prefix = str(timestamp_portion)[:-5:-1]
        return f'{key_prefix}:{action_id_snowflake}'.encode()

    @classmethod
    def _decode_action_id(cls, bigtable_key: bytes) -> int:
        """Extracts the snowflake portion of a bigtable key produced by `to_bigtable_key`"""
        _prefix, _, snowflake = bigtable_key.decode('utf-8').partition(':')
        return int(snowflake)

    @classmethod
    def _execution_result_dict_from_row(cls, row: Row) -> Dict[str, Any]:
        # row.cells doesn't have the right type information setup (at least in this version of bt), so its ignored here.
        extracted_features = row.cells['execution_result'][b'extracted_features'][0].value.decode('utf-8')  # type: ignore[attr-defined]
        error_traces = row.cells['execution_result'][b'error_traces'][0].value.decode('utf-8')  # type: ignore[attr-defined]
        # This is really dumb but I couldn't get the timestamp value to parse from bytes -> int -> epoch -> datetime
        timestamp = row.cells['execution_result'][b'timestamp'][0].timestamp  # type: ignore[attr-defined]

        execution_result_dict = {
            'id': cls._decode_action_id(row.row_key),
            'extracted_features': extracted_features,
            'error_traces': error_traces,
            'timestamp': timestamp,
            'action_data': None,
        }

        action_data = row.cells['execution_result'].get(b'action_data')  # type: ignore[attr-defined]
        if action_data:
            execution_result_dict['action_data'] = action_data[0].value.decode('utf-8')

        return execution_result_dict


class StoredExecutionResultGCS:
    _gcs_client: storage.Client | None = None
    _bucket_name: str | None = None

    @classmethod
    def _get_gcs_client(cls) -> storage.Client:
        if cls._gcs_client is None:
            from osprey.worker.lib.singletons import CONFIG

            config = CONFIG.instance()
            project_id = config.get_str('OSPREY_GCP_PROJECT_ID', 'osprey-dev')
            cls._gcs_client = storage.Client(project=project_id)
        return cls._gcs_client

    @classmethod
    def _get_bucket_name(cls) -> str:
        if cls._bucket_name is None:
            from osprey.worker.lib.singletons import CONFIG

            config = CONFIG.instance()
            cls._bucket_name = config.get_str('OSPREY_GCS_EXECUTION_RESULTS_BUCKET', 'osprey-execution-results-stg')
        return cls._bucket_name

    @classmethod
    def select_one(cls, action_id: int) -> Optional[Dict[str, Any]]:
        try:
            with metrics.timed('gcs_stored_execution_result.get_one'):
                object_name = cls._encode_action_id(action_id)
                bucket = cls._get_gcs_client().bucket(cls._get_bucket_name())
                blob = bucket.get_blob(object_name)
                if not blob:
                    metrics.increment(
                        'gcs_stored_execution_result.select_one.not_found', tags=[f'action_id:{action_id}']
                    )
                    return None

                raw_data = blob.download_as_bytes()
                data = json.loads(raw_data.decode('utf-8'))

                result = cls._execution_result_dict_from_gcs_data(data)
                return result
        except Exception as e:
            logger.error(f'Failed to retrieve execution result from GCS for action_id {action_id}: {e}')
            return None

    @classmethod
    def select_many(cls, action_ids: List[int]) -> List[Dict[str, Any]]:
        results = [
            result
            for result in gevent.pool.Pool(GCS_CONCURRENCY_LIMIT).imap(cls.select_one, action_ids)
            if result is not None
        ]

        return results

    @classmethod
    def insert(
        cls,
        action_id: int,
        extracted_features_json: str,
        error_traces_json: str,
        timestamp: datetime,
        action_data_json: str,
    ) -> None:
        try:
            with metrics.timed('gcs_stored_execution_result.insert'):
                object_name = cls._encode_action_id(action_id)
                data = {
                    'id': action_id,
                    'extracted_features': extracted_features_json,
                    'error_traces': error_traces_json,
                    'timestamp': timestamp.isoformat(),
                    'action_data': action_data_json,
                }

                json_data = json.dumps(data)
                compressed_data = gzip.compress(json_data.encode('utf-8'))

                bucket = cls._get_gcs_client().bucket(cls._get_bucket_name())
                blob = bucket.blob(object_name)

                blob.content_encoding = 'gzip'

                blob.upload_from_string(compressed_data, content_type='application/json')

        except Exception as e:
            logger.error(f'Failed to insert execution result into GCS for action_id {action_id}: {e}')

    @classmethod
    def _encode_action_id(cls, action_id_snowflake: int) -> str:
        """Constructs a GCS object key for a given snowflake using the same distribution logic as BigTable."""
        timestamp_portion = action_id_snowflake >> 22
        # reverse the last 4 characters of the timestamp to create a
        # uniformly distributed prefix space.
        key_prefix = str(timestamp_portion)[:-5:-1]
        return f'{key_prefix}:{action_id_snowflake}.json'

    @classmethod
    def _execution_result_dict_from_gcs_data(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        execution_result_dict = {
            'id': data['id'],
            'extracted_features': data['extracted_features'],
            'error_traces': data['error_traces'],
            'timestamp': datetime.fromisoformat(data['timestamp']),
            'action_data': None,
        }

        action_data = data.get('action_data')
        if action_data:
            execution_result_dict['action_data'] = action_data

        return execution_result_dict
