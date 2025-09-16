import json

from faker import Faker
from flask import Flask, Response, url_for
from flask.testing import FlaskClient
from osprey.worker.lib.snowflake import Snowflake

fake = Faker()


def test_create_query_record(app: Flask, client: 'FlaskClient[Response]') -> None:
    start = fake.past_datetime().isoformat()
    end = fake.future_datetime().isoformat()
    query_filter = 'ActionName == "user_phone_verification_completed"'

    res = client.post(
        url_for('queries.create_query_record'),
        data=json.dumps(
            {'query_filter': query_filter, 'date_range': [start, end], 'top_n': [], 'sort_order': 'DESCENDING'}
        ),
        content_type='application/json',
    )

    assert res.status_code == 200
    assert res.json['executed_by'] == 'local-dev@localhost'
    assert res.json['query_filter'] == query_filter
    assert res.json['sort_order'] == 'DESCENDING'
    assert res.json['executed_at'] == Snowflake(int(res.json['id'])).to_timestamp()


def test_get_queries(app: Flask, client: 'FlaskClient[Response]') -> None:
    res = client.get(url_for('queries.get_queries'), content_type='application/json')

    assert len(res.json) == 1
