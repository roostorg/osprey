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
    """Lists the query records this test created.

    Previously asserted a hard-coded 21, which was a census of whatever the whole suite
    had left in the table -- so it passed only when `lib/storage/tests` ran in the same
    session, and failed for any subset. Creating a known number and counting those makes
    it independent of run order and of what other modules do.
    """
    before = len(client.get(url_for('queries.get_queries'), content_type='application/json').json)

    created = 3
    for _ in range(created):
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

    res = client.get(url_for('queries.get_queries'), content_type='application/json')
    assert res.status_code == 200
    assert len(res.json) == before + created
