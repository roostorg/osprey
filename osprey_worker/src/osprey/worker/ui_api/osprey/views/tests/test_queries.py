import json

import pytest
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


@pytest.fixture
def one_query_record(client: 'FlaskClient[Response]') -> None:
    """Create exactly one query record, so a test can assert on a known table."""
    res = client.post(
        url_for('queries.create_query_record'),
        data=json.dumps(
            {
                'query_filter': fake.pystr(),
                'date_range': [fake.past_datetime().isoformat(), fake.future_datetime().isoformat()],
                'top_n': [],
                'sort_order': 'DESCENDING',
            }
        ),
        content_type='application/json',
    )
    assert res.status_code == 200


def test_get_queries(app: Flask, client: 'FlaskClient[Response]', one_query_record: None) -> None:
    """Lists the query records that exist, which is the one the fixture created.

    The count is exact rather than a lower bound because `_clear_tables` empties the
    table before each test, so nothing else can be in it.
    """
    res = client.get(url_for('queries.get_queries'), content_type='application/json')
    assert res.status_code == 200
    assert len(res.json) == 1
