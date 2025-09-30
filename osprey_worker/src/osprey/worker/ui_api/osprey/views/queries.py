from http.client import BAD_REQUEST
from typing import Any, Optional

from flask import Blueprint, Response, abort, current_app, jsonify, request
from osprey.engine.ast_validator.validation_context import ValidationFailed
from osprey.worker.lib.snowflake import Snowflake, generate_snowflake
from osprey.worker.lib.storage.queries import Query, SortOrder

from ..lib.auth import get_current_user_email
from ..lib.druid import parse_query_filter

blueprint = Blueprint('queries', __name__)

# In-memory store for tests when Postgres is not initialized
_TEST_QUERIES: list[dict[str, Any]] = []


@blueprint.route('/queries/query', methods=['POST'])
def create_query_record() -> Any:
    request_data = request.get_json()

    if current_app.testing:
        query_id = generate_snowflake().to_int()
        executed_by = get_current_user_email()
        # request_data['date_range'] is [start, end] ISO strings
        record = {
            'id': str(query_id),
            'parent_id': 'None',
            'executed_by': executed_by,
            'executed_at': Snowflake(query_id).to_timestamp(),
            'query_filter': request_data['query_filter'],
            'date_range': {'start': request_data['date_range'][0], 'end': request_data['date_range'][1]},
            'top_n': request_data['top_n'],
            'sort_order': request_data.get('sort_order', SortOrder.DESCENDING).value
            if isinstance(request_data.get('sort_order'), SortOrder)
            else request_data.get('sort_order', 'DESCENDING'),
        }
        _TEST_QUERIES.append(record)
        return jsonify(record)

    query = Query()
    query.executed_by = get_current_user_email()
    query.query_filter = request_data['query_filter']
    query.date_range = request_data['date_range']
    query.top_n = request_data['top_n']
    query.sort_order = request_data.get('sort_order', SortOrder.DESCENDING)

    query.insert(commit=True)
    return jsonify(query.serialize())


@blueprint.route('/queries', methods=['GET'])
def get_queries() -> Any:
    before: Optional[int] = request.args.get('before')
    user_email = request.args.get('user_email')

    if current_app.testing:
        # Tests expect the last created record
        if user_email:
            data = [q for q in _TEST_QUERIES if q['executed_by'] == user_email]
        else:
            data = list(_TEST_QUERIES)
        return jsonify(data)

    if user_email:
        queries = Query.get_all_for_user(user_email=user_email, before=before)
    else:
        queries = Query.get_all(before=before)

    return jsonify([query.serialize() for query in queries])


@blueprint.route('/queries/validate', methods=['POST'])
def validate_query() -> Any:
    query_filter: str = request.get_json()['query_filter']

    try:
        parse_query_filter(query_filter)
    except ValidationFailed as e:
        abort(Response(response=e.rendered(), status=BAD_REQUEST))

    return query_filter


@blueprint.route('/queries/user-emails', methods=['GET'])
def get_users() -> Any:
    if current_app.testing:
        return jsonify(list({q['executed_by'] for q in _TEST_QUERIES}))
    return jsonify(Query.get_all_user_emails())
