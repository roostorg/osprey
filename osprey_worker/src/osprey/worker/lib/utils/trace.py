import gc
from dataclasses import dataclass, field
from time import time
from typing import Dict, List, Optional, Sequence

from osprey.worker.lib.instruments import metrics
from typing_extensions import Literal

PhaseType = Literal['start', 'stop']
InfoType = Dict[str, int]

# these names reflect those collected by ddtrace
_STAT_PREFIX = 'runtime.python.gc'
_GC_RUN_GENX = f'{_STAT_PREFIX}.run'
_GC_DURATION = f'{_STAT_PREFIX}.duration'
_GC_COUNT_COLLECTED = f'{_STAT_PREFIX}.count.collected'
_GC_COUNT_UNCOLLECTABLE = f'{_STAT_PREFIX}.count.uncollectable'


@dataclass
class GCMetrics:
    """
    This will register a hook that runs before and after EVERY gc run.
    This is probably very expensive and should only run on select nodes at a time.
    """

    last_gc_start_ms: Optional[int] = None  # TODO: confirm that statd requires integers
    tags: List[str] = field(default_factory=list)

    def configure(self, service_name: str = '', extra_tags: Sequence[str] = ()) -> None:
        self.tags.extend(extra_tags)

        if service_name:
            self.tags.append(f'service:{service_name}')

    def register(self) -> None:
        for callback in gc.callbacks:
            if isinstance(callback, GCMetrics):
                raise RuntimeError('GCMetrics callback already registered')

        gc.callbacks.append(self)

    def __call__(self, phase: PhaseType, info: InfoType) -> None:
        now_ms = int(time() * 1000)
        generation = info.get('generation', 0)

        if phase == 'start':
            # count each time a given run of a generation occurs
            metrics.increment(f'{_GC_RUN_GENX}.gen{generation}', tags=self.tags)
            self.last_gc_start_ms = now_ms

        elif phase == 'stop':
            if self.last_gc_start_ms is not None:
                gc_duration_ms = now_ms - self.last_gc_start_ms
                self.last_gc_start_ms = None

                # count total time of gc in seconds
                metrics.timing(_GC_DURATION, gc_duration_ms, tags=self.tags)

            # count total number
            metrics.increment(_GC_COUNT_COLLECTED, info['collected'], tags=self.tags)
            metrics.increment(_GC_COUNT_UNCOLLECTABLE, info['uncollectable'], tags=self.tags)
