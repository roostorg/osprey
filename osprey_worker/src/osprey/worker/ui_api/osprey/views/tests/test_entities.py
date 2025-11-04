import json
from unittest import mock

import pytest
from flask import Flask, Response, url_for
from flask.testing import FlaskClient
from osprey.worker.lib.osprey_shared.labels import LabelStatus
from osprey.worker.lib.singleton import Singleton
from osprey.worker.lib.singletons import LABELS_PROVIDER
from osprey.worker.lib.snowflake import generate_snowflake
from osprey.worker.lib.storage.labels import LabelsProvider
from osprey.worker.lib.storage.tests.test_labels import MockLabelsService

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


def _mocked_init_labels_provider() -> LabelsProvider:
    return LabelsProvider(MockLabelsService())


MOCK_LABELS_PROVIDER = Singleton(_mocked_init_labels_provider)


# @mock.patch('osprey.worker.lib.singletons.LABELS_PROVIDER', new_callable=_mocked_init_labels_provider, spec_set=True)
# @mock.patch('osprey.worker.adaptor.plugin_manager.bootstrap_labels_provider', new_callable=_mocked_init_labels_provider)
@mock.patch.object(LABELS_PROVIDER, 'instance', new=_mocked_init_labels_provider)
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
    print(res.json)
    assert res.json['mutation_result'] == {'added': [], 'unchanged': [], 'removed': ['test_label'], 'updated': []}
    label = res.json['labels']['test_label']
    reason = label['reasons']['_ManuallyUpdated']
    assert reason['description'] == 'Manual update by {AdminEmail}: {Reason}'
    assert reason['features'] == {'AdminEmail': 'local-dev@localhost', 'Reason': 'test_reason'}
    assert label['status'] == LabelStatus.MANUALLY_REMOVED


@mock.patch.object(LABELS_PROVIDER, 'instance', new=_mocked_init_labels_provider)
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
    assert res.json['mutation_result'] == {'added': [], 'updated': [], 'removed': [], 'unchanged': ['test_label']}


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
