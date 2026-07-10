import abc
import logging
import os
import threading
from typing import TypeVar

import google.auth
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import pubsub_v1
from osprey.worker.lib.instruments import metrics
from osprey.worker.lib.pubsub.publisher_client import BatchPubsubPublisherClient
from pydantic import BaseModel

_PydanticModelT = TypeVar('_PydanticModelT', bound=BaseModel)

logger = logging.getLogger(__name__)


class BasePublisher(abc.ABC):
    """
    ABC Generic Publishing
    """

    @abc.abstractmethod
    def publish(self, data: _PydanticModelT, attributes: dict[str, str] | None = None) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def stop(self) -> None:
        raise NotImplementedError


class StdoutPublisher(BasePublisher):
    def publish(self, data: _PydanticModelT, attributes: dict[str, str] | None = None) -> None:
        print(data)

    def stop(self) -> None:
        pass


class NullPublisher(BasePublisher):
    def publish(self, data: _PydanticModelT, attributes: dict[str, str] | None = None) -> None:
        return

    def stop(self) -> None:
        return


class PubSubPublisher(BasePublisher):
    """Publishes Pydantic models to a Google Cloud Pub/Sub topic.

    Degrades to noop mode when the DISABLE_GCP_PUBSUB env var is set, or when GCP
    credentials cannot be resolved at construction time (e.g. local dev or adopter
    environments without GCP). In noop mode no underlying client is built and
    publish() and stop() return immediately. A one-time warning is logged at
    construction so the inert state is visible.
    """

    def __init__(
        self,
        project_id: str,
        topic_id: str,
        raise_on_error: bool = False,
        max_bytes: int = 2000000,
        max_messages: int = 250,
        max_latency: float = 1.0,
    ):
        self._topic_name = 'projects/{project_id}/topics/{topic_id}'.format(
            project_id=project_id,
            topic_id=topic_id,
        )
        self._project = project_id
        self._raise_on_error = raise_on_error
        self._tags = [f'project:{self._project}', f'topic:{self._topic_name}']
        if _gcp_pubsub_disabled():
            self._enabled = False
            logger.warning(
                'DISABLE_GCP_PUBSUB is set, PubSubPublisher disabled (project=%s, topic=%s)',
                project_id,
                topic_id,
            )
            return
        self._enabled = _check_gcp_credentials()
        if not self._enabled:
            logger.warning(
                'GCP credentials not detected, PubSubPublisher running in noop mode (project=%s, topic=%s)',
                project_id,
                topic_id,
            )
            return
        batch_settings = pubsub_v1.types.BatchSettings(
            max_bytes=max_bytes,  # default 1MB
            max_messages=max_messages,  # default 100 messages
            max_latency=max_latency,
        )
        # self._publisher = pubsub_v1.PublisherClient(batch_settings=batch_settings)
        self._publisher = BatchPubsubPublisherClient(batch_settings=batch_settings)

    def prepare_data(self, data: _PydanticModelT) -> bytes:
        """
        Convert the data to bytes for publishing. Some topics will require packing to bytes in a specific way.
        Override this method if needed.
        """
        return data.json(exclude_none=True).encode()

    def publish(self, data: _PydanticModelT, attributes: dict[str, str] | None = None) -> None:
        if not self._enabled:
            return
        if attributes is None:
            attributes = {}

        metrics.increment(f'{self.__class__.__name__}.publisher.attempt', tags=self._tags)
        try:
            processed_data = self.prepare_data(data)
            self._publisher.publish(self._topic_name, processed_data, **attributes)
        except Exception as e:
            metrics.increment(
                f'{self.__class__.__name__}.publisher.failure', tags=self._tags + [f'error:{e.__class__.__name__}']
            )
            if self._raise_on_error:
                raise e
            return
        metrics.increment(f'{self.__class__.__name__}.publisher.success', tags=self._tags)

    def stop(self) -> None:
        if not self._enabled:
            return
        self._publisher.stop()


class StrPubSubPublisher(PubSubPublisher):
    """The same as PubSubPublisher, but publishes any string"""

    def prepare_data(self, data: str) -> bytes:  # type: ignore[override]
        return data.encode()

    def publish(self, data: str, attributes: dict[str, str] | None = None) -> None:  # type: ignore[override]
        if attributes is None:
            attributes = {}

        super().publish(data, attributes)  # type: ignore[type-var]


def _gcp_pubsub_disabled() -> bool:
    return os.environ.get('DISABLE_GCP_PUBSUB', '').lower() == 'true'


_gcp_credentials_available: bool | None = None
_gcp_credentials_lock = threading.Lock()


def _check_gcp_credentials() -> bool:
    global _gcp_credentials_available
    if _gcp_credentials_available is None:
        with _gcp_credentials_lock:
            # Re-check under the lock so concurrent constructors probe google.auth.default() only once.
            if _gcp_credentials_available is None:
                try:
                    google.auth.default()
                    _gcp_credentials_available = True
                except DefaultCredentialsError:
                    _gcp_credentials_available = False
    return _gcp_credentials_available
