import csv
import os
import tempfile
from itertools import islice
from typing import Any, Dict, Generator, Optional, Protocol

from google.cloud import storage
from osprey.worker.lib.singletons import CONFIG
from werkzeug.datastructures import FileStorage

BULK_ACTION_BUCKET_NAME_ENV_KEY = 'OSPREY_BULK_ACTION_FILE_UPLOAD_BUCKET'

DEFAULT_LOCAL_STORAGE_DIR = os.path.join(tempfile.gettempdir(), 'osprey', '.storage')


class BulkActionFileManager(Protocol):
    def generate_upload_url(self, object_name: str) -> str:
        pass

    def get_bulk_action_file_stream(
        self, object_name: str, start_row: int = 0, num_rows: Optional[int] = None
    ) -> Generator[Dict[str, Any], None, None]:
        pass

    def upload_file(self, object_name: str, file_stream: FileStorage) -> None:
        pass


class NoopBulkActionFileManager(BulkActionFileManager):
    def generate_upload_url(self, object_name: str) -> str:
        return f'http://localhost:5004/bulk_action/upload/{object_name}'

    def get_bulk_action_file_stream(
        self, object_name: str, start_row: int = 0, num_rows: Optional[int] = None
    ) -> Generator[Dict[str, Any], None, None]:
        yield {}

    def upload_file(self, object_name: str, file_stream: FileStorage) -> None:
        pass


class LocalBulkActionFileManager(BulkActionFileManager):
    def __init__(self, local_dir: str = DEFAULT_LOCAL_STORAGE_DIR):
        self.local_dir = local_dir

    def generate_upload_url(self, object_name: str) -> str:
        return f'http://localhost:5004/bulk_action/upload/{object_name}'

    def get_bulk_action_file_stream(
        self, object_name: str, start_row: int = 0, num_rows: Optional[int] = None
    ) -> Generator[Dict[str, Any], None, None]:
        with open(os.path.join(self.local_dir, object_name), 'r') as f:
            reader = islice(csv.DictReader(f), start_row, start_row + num_rows if num_rows is not None else None)
            for row in reader:
                yield row

    def upload_file(self, object_name: str, file_stream: FileStorage) -> None:
        filename = os.path.join(self.local_dir, object_name)

        try:
            os.makedirs(os.path.dirname(filename))
        except OSError:
            pass

        with open(filename, 'wb') as f:
            f.write(file_stream.read())


class GCSBulkActionFileManager(BulkActionFileManager):
    def __init__(self, bucket_name: str, gcs_client: storage.Client):
        self.bucket_name = bucket_name
        self._client = gcs_client

    def generate_upload_url(self, object_name: str) -> str:
        base_url = os.getenv('BASE_URL', 'https://localhost')

        return f'{base_url}/api/bulk_action/upload/{object_name}'

    def get_bulk_action_file_stream(
        self, object_name: str, start_row: int = 0, num_rows: Optional[int] = None
    ) -> Generator[Dict[str, Any], None, None]:
        blob = self._client.get_bucket(self.bucket_name).get_blob(object_name)

        if blob is None:
            return

        with blob.open('rt') as blob_file:
            reader = islice(
                csv.DictReader(blob_file), start_row, start_row + num_rows if num_rows is not None else None
            )
            for row in reader:
                yield row

    def upload_file(self, object_name: str, file_stream: FileStorage) -> None:
        upload_bulk_action_file_to_gcs(self.bucket_name, self._client, object_name, file_stream)


def get_bulk_action_file_manager(environment: Optional[str] = None) -> BulkActionFileManager:
    if environment is None:
        environment = os.getenv('ENVIRONMENT', 'development')

    if environment in ('production', 'staging'):
        project_id = CONFIG.instance().get_str('OSPREY_GCP_PROJECT_ID', 'osprey-dev')
        bucket_name = CONFIG.instance().get_str(BULK_ACTION_BUCKET_NAME_ENV_KEY, 'osprey-bulk-action-file-upload-stg')

        gcs_client = storage.Client(project=project_id)
        return GCSBulkActionFileManager(bucket_name, gcs_client)

    return LocalBulkActionFileManager()


def upload_bulk_action_file_to_gcs(
    bucket_name: str, gcs_client: storage.Client, object_name: str, file_stream: FileStorage
) -> None:
    bucket = gcs_client.get_bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_file(file_stream, content_type=file_stream.content_type)
