import time
from datetime import UTC, datetime

import requests
from osprey.worker.lib.singletons import CONFIG


class Snowflake:
    """
    A snowflake contains a 64-bit integer ID that contains timestamp information in the first 22 bits
    """

    def __init__(self, id: int):
        self.id = id

    def to_int(self) -> int:
        return self.id

    def to_str(self) -> str:
        return str(self.id)

    def to_datetime(self) -> datetime:
        """Extract the timestamp from a Snowflake and return a UTC datetime."""
        return datetime.fromtimestamp(self.to_timestamp(), UTC)

    def to_timestamp(self) -> float:
        """
        Extract the timestamp from a snowflake and return a unix epoch timestamp.

        Uses the SNOWFLAKE_EPOCH offset if set.
        """
        epoch = CONFIG.instance().get_int('SNOWFLAKE_EPOCH', 0)
        return ((self.id >> 22) + epoch) / 1000.0

    def __str__(self) -> str:
        return self.to_str()

    def __int__(self) -> int:
        return self.to_int()

    def __repr__(self) -> str:
        return f'Snowflake({self.id})'


def generate_snowflake(retries=0) -> Snowflake:
    """
    Call the /generate endpoint and return a single snowflake.
    """
    return generate_snowflake_batch(count=1, retries=retries)[0]


def generate_snowflake_batch(count: int = 1, retries: int = 0) -> list[Snowflake]:
    """
    Call the /generate endpoint and return the list of generated snowflakes.
    """
    snowflake_api_endpoint = CONFIG.instance().expect_str('SNOWFLAKE_API_ENDPOINT')

    url = f'{snowflake_api_endpoint}/generate'

    # always try at least once
    retries = max(0, retries)

    for attempt in range(retries + 1):
        try:
            response = requests.post(url, json={'count': count}, timeout=1)
            response.raise_for_status()
            return [Snowflake(id) for id in response.json()]
        except requests.exceptions.RequestException as e:
            if attempt == retries:  # Last attempt
                raise e
            # exponential backoff starting at 50ms, capping at 3s
            time.sleep(min((attempt + 1) ** 2 * 0.05, 3))

    raise RuntimeError("shouldn't ever get here!")
