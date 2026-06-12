from contextvars import ContextVar

skip_rate_limit: ContextVar[bool] = ContextVar('skip_rate_limit', default=False)


class SkipRateLimitContext:
    """Provides the same attribute-based API as the gevent.local version
    but backed by a ContextVar so it is safe for use with asyncio tasks."""

    @property
    def skip(self) -> bool:
        return skip_rate_limit.get()

    @skip.setter
    def skip(self, value: bool) -> None:
        skip_rate_limit.set(value)


skip_rate_limit_context = SkipRateLimitContext()
