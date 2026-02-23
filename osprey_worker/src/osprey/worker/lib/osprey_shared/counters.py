from datetime import timedelta
from enum import Enum

import requests
from osprey.worker.lib.utils.flask_signing import Signer
from pydantic.main import BaseModel

BULK_READ_COUNTER_TIMEOUT = 60


class CountService(str, Enum):
    """
    Used to determine whether a request refers to a `unique` counter or a `standard` counter
    """

    unique = 'unique'
    standard = 'standard'


class ReadCounterRequest(BaseModel):
    """
    Used to fetch the count for a single counter inside a time window
    """

    key: str
    window_seconds: int
    count_service: CountService


class ReadCounterResponse(BaseModel):
    """
    The count of the key + the associated request data
    """

    key: str
    window_seconds: int
    count_service: CountService
    count: int


class BulkReadCounterRequest(BaseModel):
    """
    Used to request many counters at once
    """

    read_counter_requests: list[ReadCounterRequest]


class BulkReadCounterResponse(BaseModel):
    """
    Each index in the list is the response associated with the request at the index in `BulkReadCounterRequest`
    """

    read_counter_responses: list[ReadCounterResponse]


def read_counter(endpoint: str, signer: Signer, key: str, window: timedelta, count_service: CountService) -> int:
    """
    Returns the count of a single key
    """
    return bulk_read_counters(
        endpoint=endpoint,
        signer=signer,
        read_counter_requests=[ReadCounterRequest(key=key, window_seconds=window.seconds, count_service=count_service)],
    )[0].count


def bulk_read_counters(
    endpoint: str,
    signer: Signer,
    read_counter_requests: list[ReadCounterRequest],
    timeout: int = BULK_READ_COUNTER_TIMEOUT,
) -> list[ReadCounterResponse]:
    """
    returns a list of `ReadCounterResponse` associated index-by-index
    with `read_counter_requests: list[ReadCounterRequest]`
    """
    bulk_read_counter_request = BulkReadCounterRequest(read_counter_requests=read_counter_requests)
    data = bulk_read_counter_request.json().encode()
    headers = signer.sign(data)

    raw_resp = requests.post(f'{endpoint}counter/bulk_request', headers=headers, data=data, timeout=timeout)
    raw_resp.raise_for_status()

    return BulkReadCounterResponse.parse_obj(raw_resp.json()).read_counter_responses
