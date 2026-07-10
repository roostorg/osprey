import abc
import logging
from typing import TypeVar

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

    This is a plain transport: it assumes GCP is configured and builds its client
    eagerly. Callers that may run without GCP should build publishers via
    make_publisher(), which hands back a NullPublisher when publishing is off.
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
        batch_settings = pubsub_v1.types.BatchSettings(
            max_bytes=max_bytes,  # default 1MB
            max_messages=max_messages,  # default 100 messages
            max_latency=max_latency,
        )
        # self._publisher = pubsub_v1.PublisherClient(batch_settings=batch_settings)
        self._publisher = BatchPubsubPublisherClient(batch_settings=batch_settings)
        self._raise_on_error = raise_on_error
        self._tags = [f'project:{self._project}', f'topic:{self._topic_name}']

    def prepare_data(self, data: _PydanticModelT) -> bytes:
        """
        Convert the data to bytes for publishing. Some topics will require packing to bytes in a specific way.
        Override this method if needed.
        """
        return data.json(exclude_none=True).encode()

    def publish(self, data: _PydanticModelT, attributes: dict[str, str] | None = None) -> None:
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
        self._publisher.stop()


class StrPubSubPublisher(PubSubPublisher):
    """The same as PubSubPublisher, but publishes any string"""

    def prepare_data(self, data: str) -> bytes:  # type: ignore[override]
        return data.encode()

    def publish(self, data: str, attributes: dict[str, str] | None = None) -> None:  # type: ignore[override]
        if attributes is None:
            attributes = {}

        super().publish(data, attributes)  # type: ignore[type-var]


def make_publisher(project_id: str, topic_id: str) -> BasePublisher:
    """Build a Pub/Sub publisher, or a NullPublisher when publishing is off.

    Publishing is opt-in: unless OSPREY_PUBSUB_ENABLED is true, this returns a
    NullPublisher (the default, no-GCP experience). When it is true but GCP
    credentials cannot be resolved, PubSubPublisher construction raises; that is a
    misconfiguration, so we log, emit a one-time configuration.errors metric, and
    fall back to a NullPublisher rather than crash.
    """
    # Imported here to avoid pulling the config/singletons stack into this low-level module.
    from osprey.worker.lib.singletons import CONFIG

    if not CONFIG.instance().get_bool('OSPREY_PUBSUB_ENABLED', False):
        return NullPublisher()
    try:
        return PubSubPublisher(project_id, topic_id)
    except DefaultCredentialsError:
        logger.warning('GCP credentials not detected, publishing disabled (project=%s, topic=%s)', project_id, topic_id)
        metrics.increment(
            'configuration.errors',
            tags=[f'project:{project_id}', f'topic:{topic_id}', 'reason:gcp_credentials_missing'],
        )
        return NullPublisher()
