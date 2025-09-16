from http.client import BAD_REQUEST, NO_CONTENT
from typing import Any

from flask import Blueprint, Response, abort, jsonify, request
from osprey.worker.lib.storage.queries import Query, SavedQuery, SortOrder
from osprey.worker.ui_api.osprey.lib.abilities import (
    CanCreateAndEditSavedQueries,
    CanViewSavedQueries,
    require_ability,
)

from ..lib.auth import get_current_user_email

blueprint = Blueprint('saved-queries', __name__)


@blueprint.route('/saved-queries', methods=['POST'])
@require_ability(CanCreateAndEditSavedQueries)
def create_saved_query() -> Any:
    request_data = request.get_json()

    query = Query.get_one_with_id(request_data['query_id'])

    if not query:
        abort(BAD_REQUEST, 'Cannot find query to save')

    saved_query = SavedQuery()
    saved_query.query_id = query.id
    saved_query.name = request_data['name']
    saved_query.saved_by = get_current_user_email()

    saved_query.insert()
    return jsonify(saved_query.serialize())


@blueprint.route('/saved-queries/<saved_query_id>', methods=['GET'])
@require_ability(CanViewSavedQueries)
def get_saved_query(saved_query_id: int) -> Any:
    return jsonify(SavedQuery.get_one_with_id(saved_query_id).serialize())


@blueprint.route('/saved-queries/<saved_query_id>', methods=['PATCH'])
@require_ability(CanCreateAndEditSavedQueries)
def update_saved_query(saved_query_id: int) -> Any:
    request_data = request.get_json()
    saved_query = SavedQuery.get_one_with_id(saved_query_id)

    saved_query.name = request_data['name']
    updated_query_data = request_data.get('query')

    if updated_query_data:
        query = Query()
        query.parent_id = saved_query.query_id
        query.executed_by = get_current_user_email()
        query.query_filter = updated_query_data['query_filter']
        query.date_range = updated_query_data['date_range']
        query.top_n = updated_query_data['top_n']
        query.sort_order = updated_query_data.get('sort_order', SortOrder.DESCENDING)

        saved_query.update_with_query(query)
    else:
        saved_query.save()

    return jsonify(saved_query.serialize())


@blueprint.route('/saved-queries', methods=['GET'])
@require_ability(CanViewSavedQueries)
def get_all_saved_queries() -> Any:
    # TODO: use request arg validator when complete
    before = request.args.get('before')
    user_email = request.args.get('user_email')

    if user_email:
        saved_queries = SavedQuery.get_all_for_user(user_email=user_email, before=before)
    else:
        saved_queries = SavedQuery.get_all(before=before)

    return jsonify([saved_query.serialize() for saved_query in saved_queries])


@blueprint.route('/saved-queries/<saved_query_id>/history', methods=['GET'])
@require_ability(CanViewSavedQueries)
def get_saved_query_history(saved_query_id: int) -> Any:
    saved_query = SavedQuery.get_one_with_id(saved_query_id)
    return jsonify([query.serialize() for query in Query.get_all_for_saved_query(saved_query.query_id)])


@blueprint.route('/saved-queries/<saved_query_id>', methods=['DELETE'])
@require_ability(CanCreateAndEditSavedQueries)
def delete_saved_query(saved_query_id: int) -> Any:
    saved_query = SavedQuery.get_one_with_id(saved_query_id)
    if not saved_query:
        abort(BAD_REQUEST, 'Saved query not found')

    saved_query.delete()

    return Response(status=NO_CONTENT, response='Saved query deleted')


@blueprint.route('/saved-queries/user-emails', methods=['GET'])
def get_users() -> Any:
    return jsonify(SavedQuery.get_all_user_emails())
