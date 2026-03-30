"""Shared metric tags for worker type identification.

All metrics emitted by the worker include WORKER_TYPE_TAG so async and gevent
workers can be distinguished in Datadog dashboards.
"""

from typing import List, Sequence

WORKER_TYPE_TAG = 'worker_type:gevent'


def with_worker_tag(tags: Sequence[str]) -> List[str]:
    """Append the worker_type tag to an existing tag list."""
    return list(tags) + [WORKER_TYPE_TAG]
