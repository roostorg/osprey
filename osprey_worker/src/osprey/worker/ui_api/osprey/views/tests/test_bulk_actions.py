import json

import pytest
from flask import Flask, Response, url_for
from flask.testing import FlaskClient

config_a = {
    'main.sml': '',
    'config.yaml': json.dumps({'acl': {'users': {'local-dev@localhost': {'abilities': []}}}}),
}

config_b = {
    'main.sml': '',
    'config.yaml': json.dumps(
        {'acl': {'users': {'local-dev@localhost': {'abilities': [{'name': 'CAN_BULK_ACTION', 'allow_all': True}]}}}}
    ),
}


@pytest.mark.use_rules_sources(config_a)
def test_bulk_action_start_job_no_bulk_action_ability(app: Flask, client: 'FlaskClient[Response]') -> None:
    url = 'bulk_actions.start_bulk_job'
    res = client.post(url_for(url))
    assert res.status_code == 401


@pytest.mark.use_rules_sources(config_b)
def test_bulk_action_start_job_with_bulk_action_ability(app: Flask, client: 'FlaskClient[Response]') -> None:
    url = 'bulk_actions.start_bulk_job'
    res = client.post(
        url_for(url),
        json={
            'job_name': 'test',
            'job_description': 'test',
            'workflow_name': 'test',
            'file_name': 'test',
            'entity_type': 'user',
        },
    )

    # TODO(caidanw): update this test when the bulk action feature is re-implemented
    assert res.status_code == 501
    return

    assert res.status_code == 200
    assert res.json['id'] is not None
    assert res.json['url'] is not None


@pytest.mark.use_rules_sources(config_b)
def test_bulk_action_start_job_invalid_request(app: Flask, client: 'FlaskClient[Response]') -> None:
    url = 'bulk_actions.start_bulk_job'
    res = client.post(url_for(url), json={'not_valid': 'test'})
    assert res.status_code == 400


@pytest.mark.use_rules_sources(config_a)
def test_bulk_action_upload_completed_no_bulk_action_ability(app: Flask, client: 'FlaskClient[Response]') -> None:
    url = 'bulk_actions.upload_completed'
    res = client.post(url_for(url))
    assert res.status_code == 401


@pytest.mark.use_rules_sources(config_b)
def test_bulk_action_upload_completed_with_bulk_action_ability(app: Flask, client: 'FlaskClient[Response]') -> None:
    url = 'bulk_actions.start_bulk_job'
    res = client.post(
        url_for(url),
        json={
            'job_name': 'test',
            'job_description': 'test',
            'workflow_name': 'test',
            'file_name': 'test',
            'entity_type': 'user',
        },
    )

    # TODO(caidanw): update this test when the bulk action feature is re-implemented
    assert res.status_code == 501
    return

    assert res.status_code == 200
    assert res.json['id'] is not None
    assert res.json['url'] is not None
    job_id = res.json['id']

    url = 'bulk_actions.upload_completed'
    res = client.post(url_for(url), json={'job_id': str(job_id)})
    assert res.status_code == 200
    assert res.json['id'] == str(job_id)


@pytest.mark.use_rules_sources(config_b)
def test_bulk_action_upload_completed_invalid_request(app: Flask, client: 'FlaskClient[Response]') -> None:
    url = 'bulk_actions.upload_completed'
    res = client.post(url_for(url), json={'not_valid': 'test'})
    assert res.status_code == 400
