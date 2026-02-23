# import json
# from http.client import NO_CONTENT, OK
# from random import choice
# from collections.abc import Mapping
# from typing import Any

# import pytest
# from faker import Faker
# from flask import Flask, Response, url_for
# from flask.testing import FlaskClient

# from osprey.worker.lib.storage.queries import Query, SavedQuery, SortOrder

# config = {
#     'main.sml': '',
#     'config.yaml': json.dumps(
#         {
#             'acl': {
#                 'users': {
#                     'local-dev@localhost': {
#                         'abilities': [
#                             {'name': 'CAN_VIEW_SAVED_QUERIES', 'allow_all': True},
#                             {'name': 'CAN_CREATE_AND_EDIT_SAVED_QUERIES', 'allow_all': True},
#                         ]
#                     }
#                 }
#             }
#         }
#     ),
# }

# fake = Faker()


# @pytest.mark.use_rules_sources(config)
# def _create_query(persist: bool = True, parent_id: int | None = None) -> Query:
#     query = Query()
#     query.parent_id = parent_id
#     query.executed_by = fake.email()
#     query.query_filter = fake.pystr()
#     query.date_range = [fake.past_datetime().isoformat(), fake.future_datetime().isoformat()]
#     query.top_n = [fake.pystr() for _ in range(10)]
#     query.sort_order = choice([SortOrder.DESCENDING, SortOrder.ASCENDING])

#     if persist:
#         query.insert(commit=True)

#     return query


# @pytest.mark.use_rules_sources(config)
# def _create_saved_query() -> SavedQuery:
#     query = _create_query()

#     saved_query = SavedQuery()
#     saved_query.query_id = query.id
#     saved_query.name = fake.pystr()
#     saved_query.saved_by = fake.email()

#     saved_query.insert()

#     return saved_query


# @pytest.mark.use_rules_sources(config)
# def test_create_saved_query(app: Flask, client: 'FlaskClient[Response]') -> None:
#     query = _create_query()

#     request_data = {'query_id': query.id, 'name': fake.pystr()}

#     res = client.post(
#         url_for('saved-queries.create_saved_query'), data=json.dumps(request_data), content_type='application/json'
#     )

#     assert res.status_code == OK
#     assert res.json['saved_by'] == 'local-dev@localhost'
#     assert res.json['query_id'] == str(query.id)


# @pytest.mark.use_rules_sources(config)
# def test_get_all_saved_queries(app: Flask, client: 'FlaskClient[Response]') -> None:
#     for _ in range(3):
#         _create_saved_query()

#     res = client.get(url_for('saved-queries.get_all_saved_queries'))

#     assert len(res.json) == 4


# @pytest.mark.use_rules_sources(config)
# def test_get_saved_query(app: Flask, client: 'FlaskClient[Response]') -> None:
#     saved_query = _create_saved_query()

#     res = client.get(url_for('saved-queries.get_saved_query', saved_query_id=saved_query.id))

#     assert res.json['id'] == str(saved_query.id)


# @pytest.mark.use_rules_sources(config)
# def test_update_saved_query(app: Flask, client: 'FlaskClient[Response]') -> None:
#     saved_query = _create_saved_query()

#     assert saved_query.query.query_filter != ''

#     request_data = {
#         'query_filter': '',
#         'name': 'saved query name',
#         'date_range': ['2020-08-25T00:22:16Z', '2020-08-26T00:22:16Z'],
#         'top_n': [],
#     }

#     res = client.patch(
#         url_for('saved-queries.update_saved_query', saved_query_id=saved_query.id),
#         data=json.dumps(request_data),
#         content_type='application/json',
#     )

#     updated_saved_query = SavedQuery.get_one_with_id(saved_query.id)
#     assert res.json['query']['query_filter'] == updated_saved_query.query.query_filter


# @pytest.mark.use_rules_sources(config)
# def test_get_saved_query_history(app: Flask, client: 'FlaskClient[Response]') -> None:
#     saved_query = _create_saved_query()
#     initial_query_id = saved_query.query_id

#     updated_query_1 = _create_query(persist=False, parent_id=saved_query.query_id)
#     saved_query.update_with_query(updated_query_1)

#     updated_query_2 = _create_query(persist=False, parent_id=saved_query.query_id)
#     saved_query.update_with_query(updated_query_2)

#     res = client.get(url_for('saved-queries.get_saved_query_history', saved_query_id=saved_query.id))

#     assert len(res.json) == 3

#     latest_query = res.json[0]
#     assert str(saved_query.query_id) == latest_query['id']
#     assert str(initial_query_id) != latest_query['id']


# @pytest.mark.use_rules_sources(config)
# def test_delete_saved_query(app: Flask, client: 'FlaskClient[Response]') -> None:
#     saved_query = _create_saved_query()
#     fetched_saved_query = SavedQuery.get_one_with_id(saved_query.id)
#     assert fetched_saved_query

#     res = client.delete(url_for('saved-queries.delete_saved_query', saved_query_id=saved_query.id))

#     assert res.status_code == NO_CONTENT

#     refetched_saved_query = SavedQuery.get_one_with_id(saved_query.id)
#     assert not refetched_saved_query


# @pytest.mark.use_rules_sources(config)
# def test_get_users_in_saved_queries(app: Flask, client: 'FlaskClient[Response]') -> None:
#     saved_query = _create_saved_query()
#     email = saved_query.saved_by

#     res = client.get(url_for('saved-queries.get_users'))

#     assert email in res.json


# @pytest.mark.parametrize(
#     'method_name,method_kwargs,url,url_kwargs',
#     [
#         (
#             'post',
#             {'data': json.dumps({'query_id': 1, 'name': fake.pystr()}), 'content_type': 'application/json'},
#             'saved-queries.create_saved_query',
#             {},
#         ),
#         (
#             'patch',
#             {
#                 'data': json.dumps(
#                     {'query_filter': '', 'date_range': ['2020-08-25T00:22:16Z', '2020-08-26T00:22:16Z'], 'top_n': []}
#                 ),
#                 'content_type': 'application/json',
#             },
#             'saved-queries.update_saved_query',
#             {'saved_query_id': 1},
#         ),
#         (
#             'get',
#             {},
#             'saved-queries.get_all_saved_queries',
#             {},
#         ),
#         (
#             'delete',
#             {},
#             'saved-queries.delete_saved_query',
#             {'saved_query_id': 1},
#         ),
#         (
#             'get',
#             {},
#             'saved-queries.get_saved_query',
#             {'saved_query_id': 1},
#         ),
#         (
#             'get',
#             {},
#             'saved-queries.get_saved_query_history',
#             {'saved_query_id': 1},
#         ),
#     ],
# )
# def test_saved_query_auth_reject(
#     app: Flask,
#     client: 'FlaskClient[Response]',
#     method_name: str,
#     method_kwargs: Mapping[str, Any],
#     url: str,
#     url_kwargs: Mapping[str, Any],
# ) -> None:
#     method = getattr(client, method_name)
#     res = method(url_for(url, **url_kwargs), **method_kwargs)
#     assert res.status_code == 401, res.data
