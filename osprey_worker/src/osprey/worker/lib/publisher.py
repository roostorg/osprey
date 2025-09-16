import abc
from typing import Dict, Optional, TypeVar

from google.cloud import pubsub_v1
from osprey.worker.lib.instruments import metrics
from osprey.worker.lib.pubsub.publisher_client import BatchPubsubPublisherClient
from pydantic import BaseModel

_PydanticModelT = TypeVar('_PydanticModelT', bound=BaseModel)


class BasePublisher(abc.ABC):
    """
    ABC Generic Publishing
    """

    @abc.abstractmethod
    def publish(self, data: _PydanticModelT, attributes: Optional[Dict[str, str]] = None) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def stop(self) -> None:
        raise NotImplementedError


class StdoutPublisher(BasePublisher):
    def publish(self, data: _PydanticModelT, attributes: Optional[Dict[str, str]] = None) -> None:
        print(data)

    def stop(self) -> None:
        pass


class NullPublisher(BasePublisher):
    def publish(self, data: _PydanticModelT, attributes: Optional[Dict[str, str]] = None) -> None:
        return

    def stop(self) -> None:
        return


class PubSubPublisher(BasePublisher):
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

    def publish(self, data: _PydanticModelT, attributes: Optional[Dict[str, str]] = None) -> None:
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

    def publish(self, data: str, attributes: Optional[Dict[str, str]] = None) -> None:  # type: ignore[override]
        if attributes is None:
            attributes = {}

        super().publish(data, attributes)  # type: ignore[type-var]
