from typing import Any

from kafka import KafkaConsumer

from .gevent import FairRLock


class PatchedKafkaConsumer(KafkaConsumer):  # type: ignore
    """A KafkaConsnsumer that has had its lock patched to FairRLock, that prevents deadlocking."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._client._lock = FairRLock()
