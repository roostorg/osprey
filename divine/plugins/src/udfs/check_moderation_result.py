import os
import re
import time
from typing import Dict, Tuple

import requests
from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase
from osprey.worker.lib.osprey_shared.logging import get_logger

logger = get_logger(__name__)

_HEX64_RE = re.compile(r'^[0-9a-f]{64}$', re.IGNORECASE)

# In-memory cache: video_hash -> (result_string, expiry_timestamp)
_cache: Dict[str, Tuple[str, float]] = {}
_CACHE_TTL = 300.0  # 5 minutes
_CACHE_MAX_SIZE = 10000


class CheckModerationResultArguments(ArgumentsBase):
    video_hash: str
    """The sha256 hash of the video content to check."""


class CheckModerationResult(UDFBase[CheckModerationResultArguments, str]):
    """Queries the Divine moderation API for a video's classification result.

    Returns the action tier as a lowercase string matching the values used
    in ai_classification.sml rules: 'safe', 'review', 'age_restricted',
    'permanent_ban', or 'unknown' if the video hasn't been classified.

    Results are cached in memory for 5 minutes since classification
    results rarely change and video events are the majority of relay
    traffic.

    Configuration (environment variable):
      - ``DIVINE_MODERATION_API_URL``: Base URL of the moderation API
        (default: ``https://moderation-api.divine.video``).

    The ``/check-result/{sha256}`` endpoint is public (no auth required).
    """

    _timeout: float = 1.5

    def execute(self, execution_context: ExecutionContext, arguments: CheckModerationResultArguments) -> str:
        video_hash = arguments.video_hash
        if not video_hash:
            return 'unknown'

        if not _HEX64_RE.match(video_hash):
            logger.warning(f'Invalid video_hash format: {video_hash[:20]}...')
            return 'unknown'

        # Check cache
        now = time.monotonic()
        cached = _cache.get(video_hash)
        if cached and cached[1] > now:
            return cached[0]

        base_url = os.environ.get('DIVINE_MODERATION_API_URL', 'https://moderation-api.divine.video')

        try:
            resp = requests.get(
                f'{base_url}/check-result/{video_hash}',
                timeout=self._timeout,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            logger.exception(f'Failed to check moderation result for {video_hash[:16]}...')
            return 'unknown'

        if not data.get('moderated'):
            result = 'unknown'
        else:
            action = data.get('action', '')
            result = action.lower() if action else 'unknown'

        # Evict oldest entries if cache is full
        if len(_cache) >= _CACHE_MAX_SIZE:
            expired = [k for k, (_, exp) in _cache.items() if exp <= now]
            for k in expired:
                del _cache[k]
            # If still full after expiry sweep, clear half
            if len(_cache) >= _CACHE_MAX_SIZE:
                keys = list(_cache.keys())
                for k in keys[: len(keys) // 2]:
                    del _cache[k]

        _cache[video_hash] = (result, now + _CACHE_TTL)
        return result
