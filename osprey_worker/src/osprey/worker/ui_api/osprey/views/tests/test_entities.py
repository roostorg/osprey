import json

import pytest
from flask import Flask, Response, url_for
from flask.testing import FlaskClient
from osprey.worker.lib.osprey_shared.labels import LabelStatus
from osprey.worker.lib.snowflake import generate_snowflake

config = {
    'main.sml': '',
    'config.yaml': json.dumps(
        {
            'acl': {
                'users': {
                    'local-dev@localhost': {
                        'abilities': [
                            {'name': 'CAN_MUTATE_LABELS', 'allow_all': True},
                            {'name': 'CAN_MUTATE_ENTITIES', 'allow_all': True},
                        ]
                    }
                }
            }
        }
    ),
}


@pytest.mark.use_rules_sources(config)
def test_mutate_entity_labels(app: Flask, client: 'FlaskClient[Response]') -> None:
    res = client.post(
        url_for('entities.manual_entity_mutation', entity_type='user', entity_id=str(generate_snowflake())),
        data=json.dumps(
            {
                'mutations': [
                    {'label_name': 'test_label', 'reason': 'test_reason', 'status': LabelStatus.MANUALLY_REMOVED}
                ]
            }
        ),
        content_type='application/json',
    )
    assert res.status_code == 200, res.data
    assert res.json['mutation_result'] == {'added': [], 'dropped': [], 'removed': ['test_label'], 'unchanged': []}
    label = res.json['labels']['test_label']
    reason = label['reasons']['_ManuallyUpdated']
    assert reason['description'] == 'Manual update by {AdminEmail}: {Reason}'
    assert reason['features'] == {'AdminEmail': 'local-dev@localhost', 'Reason': 'test_reason'}
    assert label['status'] == LabelStatus.MANUALLY_REMOVED


@pytest.mark.use_rules_sources(config)
def test_mutate_entity_labels_empty_entity_str(app: Flask, client: 'FlaskClient[Response]') -> None:
    res = client.post(
        url_for('entities.manual_entity_mutation', entity_type='user', entity_id=''),
        data=json.dumps(
            {'mutations': [{'label_name': 'test_label', 'reason': 'test_reason', 'status': LabelStatus.MANUALLY_ADDED}]}
        ),
        content_type='application/json',
    )

    assert res.status_code == 200, res.data
    assert res.json['mutation_result'] == {'added': [], 'dropped': [], 'removed': [], 'unchanged': ['test_label']}


def test_mutate_entity_labels_auth_reject(app: Flask, client: 'FlaskClient[Response]') -> None:
    res = client.post(
        url_for('entities.manual_entity_mutation', entity_type='user', entity_id=str(generate_snowflake())),
        data=json.dumps(
            {
                'mutations': [
                    {'label_name': 'test_label', 'reason': 'test_reason', 'status': LabelStatus.MANUALLY_REMOVED}
                ]
            }
        ),
        content_type='application/json',
    )

    assert res.status_code == 401, res.data


def test_get_labels_auth_reject(app: Flask, client: 'FlaskClient[Response]') -> None:
    res = client.get(url_for('entities.get_labels_for_entity', entity_type='user', entity_id=str(generate_snowflake())))
    assert res.status_code == 401, res.data
