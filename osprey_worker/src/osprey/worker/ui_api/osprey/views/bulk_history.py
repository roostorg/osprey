from http.client import NOT_FOUND
from typing import Any

from flask import Blueprint, Response, abort, jsonify
from osprey.worker.lib.storage.bulk_label_task import BulkLabelTask
from osprey.worker.ui_api.osprey.lib.abilities import CanBulkLabel, require_ability

blueprint = Blueprint('bulk_history', __name__)

DEFAULT_HISTORY = 10


@blueprint.route('/bulk_history/<int:task_id>', methods=['GET'])
@require_ability(CanBulkLabel)
def get_bulk_label_task(task_id: int) -> Any:
    task = BulkLabelTask.get_one(task_id)
    if not task:
        return abort(Response(response='Unknown task id', status=NOT_FOUND))
    return jsonify([task.serialize()])


@blueprint.route('/bulk_history', methods=['GET'])
@require_ability(CanBulkLabel)
def get_last_n_bulk_label_task() -> Any:
    tasks = BulkLabelTask.get_last_n(DEFAULT_HISTORY)
    if not tasks:
        return abort(Response(response='No tasks found', status=NOT_FOUND))

    return jsonify([task.serialize() for task in tasks])
