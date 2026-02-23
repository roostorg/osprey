# ruff: noqa: E402
from osprey.worker.lib.patcher import patch_all  # isort: skip

patch_all()  # please ensure this occurs before *any* other imports !

import os  # noqa: E402
from datetime import datetime, timedelta
from typing import TextIO

import click  # noqa: E402
import simplejson as json
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.lib.storage.stored_execution_result import (
    StoredExecutionResult,
    bootstrap_execution_result_storage_service,
)
from osprey.worker.lib.utils.json import CustomJSONEncoder
from osprey.worker.ui_api.osprey.app import create_app
from osprey.worker.ui_api.osprey.lib.druid import PaginatedScanDruidQuery, PaginatedScanResult

os.environ.update(
    BIGTABLE_EMULATOR_HOST='localhost:8361',
    DEBUG='true',
    FLASK_DEBUG='1',
    FLASK_ENV='development',
    DRUID_URL='http://localhost:8082',
    POSTGRES_HOSTS='{"osprey_db": "postgresql://localhost/osprey_db"}',
)
NOW = datetime.now()
YESTERDAY = NOW - timedelta(days=1)
_DATE_FMT = '%Y-%m-%dT%H:%M:%S'


@click.group()
def cli() -> None:
    get_logger()


@cli.command()
@click.option('--start', default=YESTERDAY.strftime(_DATE_FMT), type=click.DateTime(), help='Start time of query range')
@click.option('--end', default=NOW.strftime(_DATE_FMT), type=click.DateTime(), help='End time of query range')
@click.option('--query-filter', default=None, help='Query to run')
@click.option('--action-name', default=None, help='Dump all actions with this name within the time range')
@click.option('--output', default='-', type=click.File(mode='w'), help='File to dump to')
def export_all_actions(
    start: datetime,
    end: datetime,
    output: TextIO,
    action_name: str | None,
    query_filter: str | None,
) -> None:
    """
    Queries druid for ids that match a given query or action name within a date range (default is last 24 hours).
    The event are fetched from scylla and the raw events are dumped to the given output in the following newline
    delimited json:

        {"id": ID, "timestamp": TS, "action_data": {ACTION_DATA...}}

    notes:

        --query-filter and --action-name are mutually exclusive, only use one at a time
    """
    if start > end:
        raise click.BadParameter(f'Start ({start:{_DATE_FMT}}) must be before End ({end:{_DATE_FMT}})')
    if not (query_filter or action_name):
        raise click.BadParameter('One of --query-filter or --action-name must be provided')
    if query_filter and action_name:
        raise click.BadParameter('Only specify one of --query-filter or --action-name')

    create_app()  # run all commands in an app context

    if action_name:
        query_filter = f'ActionName == "{action_name}"'

    def query_druid(next_page: str | None = None) -> PaginatedScanResult:
        assert query_filter is not None
        return PaginatedScanDruidQuery(
            start=start, end=end, query_filter=query_filter, next_page=next_page, entity=None
        ).execute()

    def row(event: StoredExecutionResult) -> dict[str, str]:
        d = event.dict()
        return {'id': d['id'], 'timestamp': d['timestamp'], 'action_data': d['action_data']}

    storage_service = bootstrap_execution_result_storage_service()
    druid_result = query_druid()
    while druid_result.action_ids:
        events = storage_service.get_many(action_ids=druid_result.action_ids)

        for event in events:
            output.writelines([json.dumps(row(event), cls=CustomJSONEncoder), '\n'])

        druid_result = query_druid(next_page=druid_result.next_page)


if __name__ == '__main__':
    cli()
