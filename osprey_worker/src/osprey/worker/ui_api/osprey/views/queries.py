from http.client import BAD_REQUEST
from typing import Any, Optional

from flask import Blueprint, Response, abort, jsonify, request
from osprey.engine.ast_validator.validation_context import ValidationFailed
from osprey.worker.lib.storage.queries import Query, SortOrder

from ..lib.auth import get_current_user_email
from ..lib.druid import parse_query_filter

blueprint = Blueprint('queries', __name__)


@blueprint.route('/queries/query', methods=['POST'])
def create_query_record() -> Any:
    request_data = request.get_json()

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
    return jsonify(Query.get_all_user_emails())
