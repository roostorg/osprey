from __future__ import annotations

import gzip
import json
from abc import ABC, abstractmethod
from datetime import datetime
from io import BytesIO
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence

import gevent
import google.cloud.storage as storage
import pytz
from gevent.threadpool import ThreadPool
from google.api_core import retry
from google.cloud.bigtable import row_filters
from google.cloud.bigtable.row import Row
from minio import Minio
from minio.error import S3Error
from osprey.engine.executor.execution_context import ExecutionResult
from osprey.worker.lib.instruments import metrics
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.lib.snowflake import Snowflake
from osprey.worker.lib.storage.bigtable import osprey_bigtable
from pydantic.main import BaseModel

logger = get_logger()


if TYPE_CHECKING:
    from osprey.worker.ui_api.osprey.lib.abilities import DataCensorAbility

BIGTABLE_CONCURRENCY_LIMIT = 100
GCS_CONCURRENCY_LIMIT = 100
MINIO_CONCURRENCY_LIMIT = 100

# Thread pool for BigTable operations to avoid gevent/gRPC conflicts
# Using a dedicated thread pool ensures gRPC calls run in real OS threads
_bigtable_threadpool = ThreadPool(maxsize=BIGTABLE_CONCURRENCY_LIMIT)


class ExecutionResultStore(ABC):
    """Abstract base class for execution result storage backends."""

    @abstractmethod
    def select_one(self, action_id: int) -> Optional[Dict[str, Any]]:
        """Retrieve a single execution result by action ID."""
        pass

    @abstractmethod
    def select_many(self, action_ids: List[int]) -> List[Dict[str, Any]]:
        """Retrieve multiple execution results by action IDs."""
        pass

    @abstractmethod
    def insert(
        self,
        action_id: int,
        extracted_features_json: str,
        error_traces_json: str,
        timestamp: datetime,
        action_data_json: str,
    ) -> None:
        """Insert an execution result."""
        pass


class ErrorTrace(BaseModel):
    rules_source_location: str
    traceback: str


class StoredExecutionResult(BaseModel):
    """
    Represents a stored execution result with methods to persist and retrieve it using a storage backend.
    """

    # NOTE: These fields must match the database column names exactly.
    id: int
    extracted_features: Dict[str, Any]
    error_traces: Sequence[ErrorTrace]
    timestamp: datetime
    action_data: Optional[Dict[str, Any]] = None

    @classmethod
    def persist_from_execution_result(
        cls, execution_result: ExecutionResult, storage_backend: ExecutionResultStore
    ) -> None:
        """Persist execution result using the provided storage backend."""
        storage_backend.insert(
            action_id=execution_result.action.action_id,
            extracted_features_json=execution_result.extracted_features_json,
            error_traces_json=execution_result.error_traces_json,
            action_data_json=execution_result.action.data_json,
            timestamp=execution_result.action.timestamp,
        )

    @classmethod
    def get_one_with_action_data(
        cls,
        event_record_id: int,
        storage_backend: ExecutionResultStore,
        data_censor_abilities: Sequence[Optional[DataCensorAbility[Any, Any]]] = (),
    ) -> Optional['StoredExecutionResult']:
        """Get execution result from the provided storage backend."""
        result = storage_backend.select_one(event_record_id)
        if result:
            return StoredExecutionResult.parse_from_query_result(result, data_censor_abilities)
        return None

    @classmethod
    def get_many(
        cls,
        action_ids: List[int],
        storage_backend: ExecutionResultStore,
        data_censor_abilities: Sequence[Optional[DataCensorAbility[Any, Any]]] = (),
    ) -> List['StoredExecutionResult']:
        """Get execution results from the provided storage backend."""
        results = storage_backend.select_many(action_ids)

        return sorted(
            [StoredExecutionResult.parse_from_query_result(result, data_censor_abilities) for result in results],
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
class StoredExecutionResultBigTable(ExecutionResultStore):
    retry_policy = retry.Retry(initial=1.0, maximum=2.0, multiplier=1.25, deadline=120.0)

    def select_one(self, action_id: int) -> Optional[Dict[str, Any]]:
        # Run read_row in a real thread to avoid gevent/gRPC conflicts
        def _read_row():
            return osprey_bigtable.table('stored_execution_result').read_row(
                StoredExecutionResultBigTable._encode_action_id(action_id), row_filters.CellsColumnLimitFilter(1)
            )

        row = _bigtable_threadpool.apply(_read_row)
        if not row:
            return None

        return StoredExecutionResultBigTable._execution_result_dict_from_row(row)

    # TODO: Add `select_*_minimal` methods

    def select_many(self, action_ids: List[int]) -> List[Dict[str, Any]]:
        return [
            row
            for row in gevent.pool.Pool(BIGTABLE_CONCURRENCY_LIMIT).imap(self.select_one, action_ids)
            if row is not None
        ]

    def insert(
        self,
        action_id: int,
        extracted_features_json: str,
        error_traces_json: str,
        timestamp: datetime,
        action_data_json: str,
    ) -> None:
        row = osprey_bigtable.table('stored_execution_result').row(
            StoredExecutionResultBigTable._encode_action_id(action_id)
        )
        row.set_cell('execution_result', b'extracted_features', extracted_features_json.encode(), timestamp=timestamp)
        row.set_cell('execution_result', b'error_traces', error_traces_json.encode(), timestamp=timestamp)
        row.set_cell('execution_result', b'timestamp', timestamp.isoformat().encode(), timestamp=timestamp)
        row.set_cell('execution_result', b'action_data', action_data_json.encode(), timestamp=timestamp)

        # Run mutate_rows in a real thread to avoid gevent/gRPC conflicts
        # This prevents the greenlet context-switching issue with gRPC streaming calls
        def _mutate_rows():
            osprey_bigtable.table('stored_execution_result').mutate_rows([row], retry=self.retry_policy)

        _bigtable_threadpool.apply(_mutate_rows)

    @staticmethod
    def _encode_action_id(action_id_snowflake: int) -> bytes:
        """Constructs a bigtable key for a given snowflake."""
        key_prefix = Snowflake(action_id_snowflake).to_key_prefix()
        return f'{key_prefix}:{action_id_snowflake}'.encode()

    @staticmethod
    def _decode_action_id(bigtable_key: bytes) -> int:
        """Extracts the snowflake portion of a bigtable key produced by `to_bigtable_key`"""
        _prefix, _, snowflake = bigtable_key.decode('utf-8').partition(':')
        return int(snowflake)

    @staticmethod
    def _execution_result_dict_from_row(row: Row) -> Dict[str, Any]:
        # row.cells doesn't have the right type information setup (at least in this version of bt), so its ignored here.
        extracted_features = row.cells['execution_result'][b'extracted_features'][0].value.decode('utf-8')  # type: ignore[attr-defined]
        error_traces = row.cells['execution_result'][b'error_traces'][0].value.decode('utf-8')  # type: ignore[attr-defined]
        # This is really dumb but I couldn't get the timestamp value to parse from bytes -> int -> epoch -> datetime
        timestamp = row.cells['execution_result'][b'timestamp'][0].timestamp  # type: ignore[attr-defined]

        execution_result_dict = {
            'id': StoredExecutionResultBigTable._decode_action_id(row.row_key),
            'extracted_features': extracted_features,
            'error_traces': error_traces,
            'timestamp': timestamp,
            'action_data': None,
        }

        action_data = row.cells['execution_result'].get(b'action_data')  # type: ignore[attr-defined]
        if action_data:
            execution_result_dict['action_data'] = action_data[0].value.decode('utf-8')

        return execution_result_dict


class StoredExecutionResultGCS(ExecutionResultStore):
    def __init__(self):
        self._gcs_client: storage.Client | None = None
        self._bucket_name: str | None = None

    def _get_gcs_client(self) -> storage.Client:
        if self._gcs_client is None:
            from osprey.worker.lib.singletons import CONFIG

            config = CONFIG.instance()
            project_id = config.get_str('OSPREY_GCP_PROJECT_ID', 'osprey-dev')
            self._gcs_client = storage.Client(project=project_id)
        return self._gcs_client

    def _get_bucket_name(self) -> str:
        if self._bucket_name is None:
            from osprey.worker.lib.singletons import CONFIG

            config = CONFIG.instance()
            self._bucket_name = config.get_str('OSPREY_GCS_EXECUTION_RESULTS_BUCKET', 'osprey-execution-results-stg')
        return self._bucket_name

    def select_one(self, action_id: int) -> Optional[Dict[str, Any]]:
        try:
            with metrics.timed('gcs_stored_execution_result.get_one'):
                object_name = StoredExecutionResultGCS._encode_action_id(action_id)
                bucket = self._get_gcs_client().bucket(self._get_bucket_name())
                blob = bucket.get_blob(object_name)
                if not blob:
                    metrics.increment(
                        'gcs_stored_execution_result.select_one.not_found', tags=[f'action_id:{action_id}']
                    )
                    return None

                raw_data = blob.download_as_bytes()
                data = json.loads(raw_data.decode('utf-8'))

                result = StoredExecutionResultGCS._execution_result_dict_from_gcs_data(data)
                return result
        except Exception as e:
            logger.error(f'Failed to retrieve execution result from GCS for action_id {action_id}: {e}')
            return None

    def select_many(self, action_ids: List[int]) -> List[Dict[str, Any]]:
        results = [
            result
            for result in gevent.pool.Pool(GCS_CONCURRENCY_LIMIT).imap(self.select_one, action_ids)
            if result is not None
        ]

        return results

    def insert(
        self,
        action_id: int,
        extracted_features_json: str,
        error_traces_json: str,
        timestamp: datetime,
        action_data_json: str,
    ) -> None:
        try:
            with metrics.timed('gcs_stored_execution_result.insert'):
                object_name = StoredExecutionResultGCS._encode_action_id(action_id)
                data = {
                    'id': action_id,
                    'extracted_features': extracted_features_json,
                    'error_traces': error_traces_json,
                    'timestamp': timestamp.isoformat(),
                    'action_data': action_data_json,
                }

                json_data = json.dumps(data)
                compressed_data = gzip.compress(json_data.encode('utf-8'))

                bucket = self._get_gcs_client().bucket(self._get_bucket_name())
                blob = bucket.blob(object_name)

                blob.content_encoding = 'gzip'

                blob.upload_from_string(compressed_data, content_type='application/json')

        except Exception as e:
            logger.error(f'Failed to insert execution result into GCS for action_id {action_id}: {e}')

    @staticmethod
    def _encode_action_id(action_id_snowflake: int) -> str:
        """Constructs a GCS object key for a given snowflake using the same distribution logic as BigTable."""
        key_prefix = Snowflake(action_id_snowflake).to_key_prefix()
        return f'{key_prefix}:{action_id_snowflake}.json'

    @staticmethod
    def _execution_result_dict_from_gcs_data(data: Dict[str, Any]) -> Dict[str, Any]:
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


class StoredExecutionResultMinIO(ExecutionResultStore):
    def __init__(self, endpoint: str, access_key: str, secret_key: str, secure: bool, bucket_name: str):
        self._minio_client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
        self._bucket_name = bucket_name

    def select_one(self, action_id: int) -> Optional[Dict[str, Any]]:
        try:
            with metrics.timed('minio_stored_execution_result.get_one'):
                object_name = StoredExecutionResultMinIO._encode_action_id(action_id)

                try:
                    response = self._minio_client.get_object(self._bucket_name, object_name)
                    raw_data = response.read()
                    response.close()
                    response.release_conn()

                    data = json.loads(raw_data.decode('utf-8'))
                    result = StoredExecutionResultMinIO._execution_result_dict_from_minio_data(data)
                    return result

                except S3Error as e:
                    if e.code == 'NoSuchKey':
                        metrics.increment(
                            'minio_stored_execution_result.select_one.not_found', tags=[f'action_id:{action_id}']
                        )
                        return None
                    raise

        except Exception as e:
            logger.error(f'Failed to retrieve execution result from MinIO for action_id {action_id}: {e}')
            return None

    def select_many(self, action_ids: List[int]) -> List[Dict[str, Any]]:
        results = [
            result
            for result in gevent.pool.Pool(MINIO_CONCURRENCY_LIMIT).imap(self.select_one, action_ids)
            if result is not None
        ]
        return results

    def insert(
        self,
        action_id: int,
        extracted_features_json: str,
        error_traces_json: str,
        timestamp: datetime,
        action_data_json: str,
    ) -> None:
        try:
            with metrics.timed('minio_stored_execution_result.insert'):
                object_name = StoredExecutionResultMinIO._encode_action_id(action_id)
                data = {
                    'id': action_id,
                    'extracted_features': extracted_features_json,
                    'error_traces': error_traces_json,
                    'timestamp': timestamp.isoformat(),
                    'action_data': action_data_json,
                }

                json_data = json.dumps(data)

                data_stream = BytesIO(json_data.encode('utf-8'))

                self._minio_client.put_object(
                    self._bucket_name,
                    object_name,
                    data_stream,
                    length=len(json_data.encode('utf-8')),
                    content_type='application/json',
                )

        except Exception as e:
            logger.error(f'Failed to insert execution result into MinIO for action_id {action_id}: {e}')

    @staticmethod
    def _encode_action_id(action_id_snowflake: int) -> str:
        """Constructs a MinIO object key for a given snowflake using the same distribution logic as BigTable."""
        key_prefix = Snowflake(action_id_snowflake).to_key_prefix()
        return f'{key_prefix}:{action_id_snowflake}.json'

    @staticmethod
    def _execution_result_dict_from_minio_data(data: Dict[str, Any]) -> Dict[str, Any]:
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


class ExecutionResultStorageService:
    """Service class that provides execution result operations with a configured backend."""

    def __init__(self, storage_backend: ExecutionResultStore):
        self._storage_backend = storage_backend

    def persist_from_execution_result(self, execution_result: ExecutionResult) -> None:
        """Persist execution result using the configured storage backend."""
        StoredExecutionResult.persist_from_execution_result(execution_result, self._storage_backend)

    def get_one_with_action_data(
        self, event_record_id: int, data_censor_abilities: Sequence[Optional[DataCensorAbility[Any, Any]]] = ()
    ) -> Optional[StoredExecutionResult]:
        """Get execution result from the configured storage backend."""
        return StoredExecutionResult.get_one_with_action_data(
            event_record_id, self._storage_backend, data_censor_abilities
        )

    def get_many(
        self, action_ids: List[int], data_censor_abilities: Sequence[Optional[DataCensorAbility[Any, Any]]] = ()
    ) -> List[StoredExecutionResult]:
        """Get execution results from the configured storage backend."""
        return StoredExecutionResult.get_many(action_ids, self._storage_backend, data_censor_abilities)


def bootstrap_execution_result_storage_service() -> ExecutionResultStorageService:
    """Create an ExecutionResultStorageService with the configured storage backend."""
    from osprey.worker.adaptor.plugin_manager import bootstrap_execution_result_store
    from osprey.worker.lib.singletons import CONFIG

    config = CONFIG.instance()
    storage_backend = bootstrap_execution_result_store(config)
    return ExecutionResultStorageService(storage_backend)
