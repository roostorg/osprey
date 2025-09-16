from __future__ import annotations

import logging
import random
import time
from typing import Callable, Optional, Tuple, Type

import requests
from requests.exceptions import ConnectionError, ReadTimeout

DEFAULT_NUM_RETRIES = 5
logger = logging.getLogger(__name__)


class SessionWithRetries:
    def __init__(
        self,
        exceptions: Tuple[Type[Exception], ...] = (ConnectionError, ReadTimeout),
        min_delay: float = 0.1,
        max_delay: float = 2,
        jitter: bool = True,
        default_retries: int = DEFAULT_NUM_RETRIES,
        version: str = 'unknown',
        environment: str = 'unknown',
        raise_for_status: bool = False,
        predicate: Optional[Callable[[Exception], bool]] = None,
        pool_connections: int = requests.adapters.DEFAULT_POOLSIZE,
        pool_maxsize: int = requests.adapters.DEFAULT_POOLSIZE,
        connection_retries: int = requests.adapters.DEFAULT_RETRIES,
    ) -> None:
        session = requests.Session()
        session.headers['User-Agent'] = f'Osprey HTTP Client/{version} Environment/{environment}'

        # The default adapters are mounted for HTTP and HTTPS with requests.adapters.DEFAULT_POOLSIZE
        # pool_connections and pool_maxsize, so this is mimicking that behavior with the option
        # to override the pool sizes.
        session.mount(
            'https://',
            requests.adapters.HTTPAdapter(
                pool_connections=pool_connections, pool_maxsize=pool_maxsize, max_retries=connection_retries
            ),
        )
        session.mount(
            'http://',
            requests.adapters.HTTPAdapter(
                pool_connections=pool_connections, pool_maxsize=pool_maxsize, max_retries=connection_retries
            ),
        )

        self.session = session
        self.exceptions = exceptions
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.jitter = jitter
        self.default_retries = default_retries
        self.raise_for_status = raise_for_status
        self.predicate = predicate

    def _with_retries(self, func, *args, **kwargs):
        num_retries = kwargs.pop('_retries', self.default_retries)
        fails = 0
        current_delay = self.min_delay

        for i in range(num_retries):
            try:
                rv = func(*args, **kwargs)
                if self.raise_for_status:
                    rv.raise_for_status()
                return rv
            except self.exceptions as e:
                if i == (num_retries - 1):
                    raise
                if self.predicate and not self.predicate(e):
                    raise

                # Calculate backoff inline (formerly in Backoff.fail())
                current_delay = self.min_delay * (2**fails)
                fails += 1

                if self.max_delay:
                    current_delay = min(current_delay, self.max_delay)

                if self.jitter:
                    half = current_delay / 2.0
                    current_delay = half + (half * random.random())

                current_delay = round(current_delay, 2)
                time.sleep(current_delay)

    def request(self, *args, **kwargs) -> requests.Response:
        return self._with_retries(self.session.request, *args, **kwargs)

    def get(self, *args: object, **kwargs: object) -> requests.Response:
        return self._with_retries(self.session.get, *args, **kwargs)

    def post(self, *args: object, **kwargs: object) -> requests.Response:
        return self._with_retries(self.session.post, *args, **kwargs)

    def put(self, *args: object, **kwargs: object) -> requests.Response:
        return self._with_retries(self.session.put, *args, **kwargs)

    def patch(self, *args: object, **kwargs: object) -> requests.Response:
        return self._with_retries(self.session.patch, *args, **kwargs)

    def delete(self, *args: object, **kwargs: object) -> requests.Response:
        return self._with_retries(self.session.delete, *args, **kwargs)
