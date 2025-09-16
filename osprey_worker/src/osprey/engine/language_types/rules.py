from dataclasses import dataclass
from typing import Dict

from osprey.engine.utils.types import add_slots

from .post_execution_convertible import PostExecutionConvertible


@add_slots
@dataclass(frozen=True)
class RuleT(PostExecutionConvertible[bool]):
    name: str
    value: bool
    description: str
    features: Dict[str, str]

    def to_post_execution_value(self) -> bool:
        return self.value
