import abc
from dataclasses import dataclass
from typing import Any


@dataclass
class ActionProtoDeserializeResult:
    data: dict[str, Any]
    action_id: int
    action_name: str


class ActionProtoDeserializer(abc.ABC):
    @abc.abstractmethod
    def proto_bytes_to_dict(cls, data: bytes) -> ActionProtoDeserializeResult:
        """Deserialize action protobuf from bytes and convert to JSON."""
        raise NotImplementedError
