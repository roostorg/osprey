"""Base acking context classes — no gevent dependency.

These classes are used by both the sync (gevent) and async (asyncio) workers.
The gevent-dependent PubSub acking contexts remain in acking_contexts.py.
"""

import abc
from datetime import datetime
from types import TracebackType
from typing import Dict, Generic, Optional, Type, TypeVar, Union

from osprey.rpc.common.v1.verdicts_pb2 import Verdicts

_T = TypeVar('_T')


# TODO: support NACK
class BaseAckingContext(abc.ABC, Generic[_T]):
    """An acking context for handling single actions from input streams."""

    def __init__(self, item: _T) -> None:
        super().__init__()
        self._item: _T = item
        self._should_nack = False
        self._publish_time = datetime.now()
        self._attributes: Optional[Dict[str, str]] = None

    @abc.abstractmethod
    def _ack(self) -> None:
        """Acknowledges the message or item that this Acking Context holds."""

        raise NotImplementedError

    @abc.abstractmethod
    def _nack(self) -> None:
        """NACKs the message or item that this Acking Context holds."""

        raise NotImplementedError

    @property
    def attributes(self) -> Optional[Dict[str, str]]:
        return self._attributes

    def mark_as_nack(self) -> None:
        self._should_nack = True

    def __enter__(self) -> _T:
        return self._item

    def __exit__(
        self,
        exc_type: Union[Type[BaseException], None],
        exc_value: Union[BaseException, None],
        exc_traceback: Union[TracebackType, None],
    ) -> None:
        if self._should_nack:
            self._nack()
        else:
            self._ack()

    @property
    def publish_time(self) -> datetime:
        return self._publish_time


class NoopAckingContext(BaseAckingContext[_T]):
    """A context manager for handling single actions require no acking operations from input streams."""

    def _ack(self) -> None:
        return

    def _nack(self) -> None:
        return


class VerdictsAckingContext(NoopAckingContext[_T]):
    """
    A context manager for storing verdicts from the rules sink inside of a NoopAckingContext :3

    This is used to send verdicts back to the Osprey Coordinator, if any were captured~
    """

    def __init__(self, item: _T) -> None:
        super().__init__(item)
        self._verdicts: Optional[Verdicts] = None

    def set_verdicts(self, verdicts: Verdicts) -> None:
        self._verdicts = verdicts

    def get_verdicts(self) -> Optional[Verdicts]:
        return self._verdicts
