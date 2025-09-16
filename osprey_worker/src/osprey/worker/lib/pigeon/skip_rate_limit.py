from typing import TypeVar

from gevent.local import local

# Type variables for argument and return types
R = TypeVar('R')


class SkipRateLimitContext(local):  # type: ignore[misc]
    skip = False


skip_rate_limit_context = SkipRateLimitContext()
