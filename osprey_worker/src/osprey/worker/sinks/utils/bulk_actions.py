from dataclasses import dataclass
from typing import List

from osprey.engine.executor.execution_context import Action
from osprey.engine.utils.types import add_slots


@add_slots
@dataclass
class BulkActions:
    actions: List[Action]
