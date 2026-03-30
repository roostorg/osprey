"""Shared metric tags for the async worker.

All metrics emitted by the async worker include WORKER_TYPE_TAG so they can be
distinguished from gevent worker metrics in Datadog dashboards.
"""

from typing import List, Sequence

WORKER_TYPE_TAG = 'worker_type:async'


def with_worker_tag(tags: Sequence[str]) -> List[str]:
    """Append the worker_type tag to an existing tag list."""
    return list(tags) + [WORKER_TYPE_TAG]
