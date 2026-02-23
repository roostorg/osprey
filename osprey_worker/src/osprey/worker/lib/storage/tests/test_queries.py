from __future__ import absolute_import

from collections.abc import Iterator
from random import choice

import pytest
from faker import Faker
from intervals.interval import DateTimeInterval
from osprey.worker.lib.snowflake import Snowflake
from osprey.worker.lib.storage.postgres import scoped_session
from osprey.worker.lib.storage.queries import Query, SavedQuery, SortOrder
from sqlalchemy.orm.session import Session

fake = Faker()


@pytest.fixture(autouse=True)
def sqlalchemy_session() -> Iterator[Session]:
    with scoped_session() as session:
        yield session


def _create_query(executed_by: str | None = None, parent_id: int | None = None, insert: bool | None = False) -> Query:
    query = Query()
    query.parent_id = parent_id
    query.executed_by = executed_by if executed_by else fake.email()
    query.query_filter = fake.pystr()
    query.date_range = [fake.past_datetime().isoformat(), fake.future_datetime().isoformat()]
    query.top_n = [fake.pystr() for _ in range(10)]
    query.sort_order = choice([SortOrder.DESCENDING, SortOrder.ASCENDING])

    if insert:
        query.insert(commit=True)

    return query


def _create_saved_query(query: Query, saved_by: str | None = None) -> SavedQuery:
    saved_query = SavedQuery()
    saved_query.query_id = query.id
    saved_query.name = fake.pystr()
    saved_query.saved_by = saved_by if saved_by else fake.email()

    return saved_query


def test_insert_one_query() -> None:
    query = _create_query(insert=True)

    assert query.id
    assert isinstance(query.date_range, DateTimeInterval)


def test_get_all_for_user() -> None:
    user = fake.email()

    for i in range(3):
        _create_query(executed_by=user, insert=True)

    for i in range(4):
        _create_query(insert=True)

    user_queries = Query.get_all_for_user(user)
    assert len(user_queries) == 3


def test_get_all_users_emails_in_queries() -> None:
    user_email = fake.email()
    _create_query(executed_by=user_email, insert=True)

    assert user_email in Query.get_all_user_emails()


def test_get_all_for_saved_query() -> None:
    query_one = _create_query(insert=True)

    saved_query = _create_saved_query(query_one)
    saved_query.insert()

    query_two = _create_query(parent_id=saved_query.query_id)
    saved_query.update_with_query(query_two)

    query_three = _create_query(parent_id=saved_query.query_id)
    saved_query.update_with_query(query_three)

    queries = [query_three, query_two, query_one]
    history = Query.get_all_for_saved_query(saved_query.query_id)

    assert len(history) == 3
    for idx, query in enumerate(history):
        assert queries[idx] == query


def test_serialize_query() -> None:
    query = _create_query(insert=True)
    serialized_query = query.serialize()

    assert serialized_query['executed_at'] == Snowflake(query.id).to_timestamp()
    assert serialized_query['date_range']['start']
    assert serialized_query['date_range']['end']


def test_insert_one_saved_query() -> None:
    query = _create_query(insert=True)

    saved_query = _create_saved_query(query)
    saved_query.insert()

    assert saved_query.query == query


def test_update_one_saved_query() -> None:
    query = _create_query(insert=True)

    saved_query = _create_saved_query(query)
    saved_query.insert()

    assert saved_query.query_id == query.id

    updated_query = _create_query(parent_id=query.id)
    saved_query.update_with_query(updated_query)

    assert saved_query.query_id == updated_query.id
    assert saved_query.query == updated_query


def test_get_one_saved_query_with_id() -> None:
    query = _create_query(insert=True)

    saved_query = _create_saved_query(query)
    saved_query.insert()

    retrieved_saved_query = SavedQuery.get_one_with_id(saved_query.id)
    assert retrieved_saved_query == saved_query


def test_get_all_users_emails_in_saved_queries() -> None:
    query = _create_query(insert=True)

    user_email = fake.email()
    saved_query = _create_saved_query(query=query, saved_by=user_email)
    saved_query.insert()

    assert user_email in SavedQuery.get_all_user_emails()


def test_serialize_saved_query() -> None:
    query = _create_query(insert=True)

    saved_query = _create_saved_query(query)
    saved_query.insert()

    serialized_saved_query = saved_query.serialize()
    assert serialized_saved_query['saved_at'] == Snowflake(saved_query.id).to_timestamp()
    assert serialized_saved_query['query'] == query.serialize()


def test_delete_saved_query() -> None:
    query = _create_query(insert=True)

    saved_query = _create_saved_query(query)
    saved_query.insert()

    saved_query = SavedQuery.get_one_with_query_id(query.id)
    saved_query.delete()

    deleted_query = SavedQuery.get_one_with_query_id(query.id)
    assert deleted_query is None
