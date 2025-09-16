import json
from datetime import datetime
from unittest.mock import Mock, patch

import pytest
from flask import Flask, Response, url_for
from flask.testing import FlaskClient
from osprey.rpc.labels.v1.service_pb2 import LabelStatus
from osprey.worker.lib.storage.bulk_label_task import BulkLabelTask

config_a = {
    'main.sml': '',
    'config.yaml': json.dumps({'acl': {'users': {'local-dev@localhost': {'abilities': []}}}}),
}

config_b = {
    'main.sml': '',
    'config.yaml': json.dumps(
        {'acl': {'users': {'local-dev@localhost': {'abilities': [{'name': 'CAN_BULK_LABEL', 'allow_all': True}]}}}}
    ),
}


@pytest.fixture()
def bulk_label_task() -> BulkLabelTask:
    task = BulkLabelTask.enqueue(
        initiated_by='test@test.com',
        query={
            'query_filter': 'fake',
            'start': datetime.timestamp(datetime.now()),
            'end': datetime.timestamp(datetime.now()),
        },
        excluded_entities=[],
        expected_total_entities_to_label=10,
        no_limit=False,
        dimension='UserId',
        label_name='fake',
        label_reason='fake',
        label_status=LabelStatus.MANUALLY_ADDED,
        label_expiry=None,
    )

    return task


@pytest.mark.use_rules_sources(config_a)
def test_get_bulk_label_task_missing_ability(app: Flask, client: 'FlaskClient[Response]') -> None:
    res = client.get(url_for('bulk_history.get_bulk_label_task', task_id=1), content_type='application/json')

    assert res.status_code == 401
    assert res.data.decode('utf-8') == "User `local-dev@localhost` doesn't have ability `CAN_BULK_LABEL`"


@pytest.mark.use_rules_sources(config_b)
@patch.object(BulkLabelTask, 'get_one')
def test_get_bulk_label_task(
    bulk_get_one: Mock, app: Flask, client: 'FlaskClient[Response]', bulk_label_task: BulkLabelTask
) -> None:
    bulk_get_one.return_value = bulk_label_task
    res = client.get(url_for('bulk_history.get_bulk_label_task', task_id=1), content_type='application/json')

    assert res.status_code == 200
    assert len(res.json) == 1
