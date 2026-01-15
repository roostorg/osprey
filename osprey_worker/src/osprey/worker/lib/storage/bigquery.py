import time
from typing import Any, Optional, Sequence, Union
from uuid import uuid4

from google.cloud import bigquery
from google.cloud.bigquery.client import Client
from google.cloud.bigquery.job import QueryJob, QueryJobConfig, QueryPriority
from google.cloud.bigquery.table import RowIterator
from osprey.worker.lib.ddtrace_utils import current_span, trace_wrap

AbstractQueryParameterT = Union[bigquery.ScalarQueryParameter, bigquery.ArrayQueryParameter]
QueryParamsT = Sequence[AbstractQueryParameterT]
QueryExecutorRetT = list[dict[str, Any]]

TIMEOUT_MS = 60 * 1000
TIMEOUT_WAIT_INCREMENT_S = 1


class BigQueryError(Exception):
    """Something went wrong querying BigQuery and no data was returned."""

    def __init__(self, job):
        self.job = job
        self.errors = job.errors

    def __str__(self) -> str:
        state = self.job.state
        return (
            f'Job {self.job.job_id!r} with state {state!r} failed with errors {self.errors!r}.'
            f'\nQuery: {self.job.query!r}'
            f'\nQuery params: {self.job.query_parameters!r}'
        )


@trace_wrap()
def execute_query(
    client: Client,
    query: str,
    timeout_ms: int,
    query_parameters: QueryParamsT = (),
    job_name_prefix: Optional[str] = None,
) -> QueryExecutorRetT:
    """
    Runs a query with the provided client
    :param client: Client to use to execute the query
    :param query: Query to run
    :param timeout_ms: Amount of time in milliseconds to wait for the query to complete
    :param query_parameters: Parameters to be substituted into the query
    :param job_name_prefix: A prefix (not including a trailing _) to use for the BigQuery Job ID
    :return: Rows from running the query
    """
    current_span().resource = query

    prefix = f'{job_name_prefix}_' if job_name_prefix else ''
    job_name = prefix + str(uuid4())
    job_config = QueryJobConfig(query_parameters=query_parameters)
    job_config.allow_large_results = True
    job_config.priority = QueryPriority.BATCH

    job = client.query(job_id=job_name, query=query, job_config=job_config)
    # note that the query job begins automatically

    _wait_for_job(job, timeout_ms)
    row_iterator: RowIterator = job.result()

    if not row_iterator.schema and not row_iterator.total_rows:
        # If you want to have DELETEs return the # of rows deleted use `select @@row_count;`
        # https://cloud.google.com/bigquery/docs/reference/system-variables
        return []

    rows = list(row_iterator)

    if not row_iterator.schema:
        raise BigQueryError(job)
    names = [field.name for field in row_iterator.schema]

    return [dict(zip(names, row)) for row in rows]


def _wait_for_job(job: QueryJob, timeout_ms: int, timeout_wait_increment_s: int = TIMEOUT_WAIT_INCREMENT_S) -> None:
    remaining_timeout = timeout_ms or 0
    job.reload()

    while job.state != 'DONE' and remaining_timeout > 0:
        time.sleep(timeout_wait_increment_s)
        job.reload()
        remaining_timeout -= timeout_wait_increment_s * 1000

    if job.state != 'DONE' or job.error_result:
        raise BigQueryError(job)
