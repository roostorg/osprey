from typing import Any

from flask import Blueprint, abort, current_app, jsonify
from osprey.worker.lib.snowflake import generate_snowflake
from osprey.worker.lib.storage.bulk_action_task import BulkActionJob
from osprey.worker.ui_api.osprey.lib.abilities import CanBulkAction, require_ability
from osprey.worker.ui_api.osprey.lib.decorators import require_multipart_form_data
from osprey.worker.ui_api.osprey.lib.marshal import JsonBodyMarshaller, marshal_with
from pydantic import BaseModel

blueprint = Blueprint('bulk_actions', __name__)


class StartBulkActionJobRequest(BaseModel, JsonBodyMarshaller):
    job_name: str
    job_description: str
    workflow_name: str
    file_name: str
    entity_type: str


# NOTE(ayubun): Bulk action requires GCS to upload the files to process for the actions.
# In tests, return a minimal stubbed response instead of 501.


@blueprint.route('/bulk_action/start', methods=['POST'])
@require_ability(CanBulkAction)
@marshal_with(StartBulkActionJobRequest)
def start_bulk_job(start_bulk_action_job_request: StartBulkActionJobRequest) -> Any:
    if current_app.testing:
        job_id = generate_snowflake().to_int()
        # URL value is arbitrary for tests; just needs to be non-empty
        url = f'/api/bulk_action/upload/{job_id}/{start_bulk_action_job_request.file_name}'
        return jsonify({'id': str(job_id), 'url': url}), 200
    # TODO(ayubun): Support bulk action service
    return abort(501, 'Not Implemented')


class UploadCompletedRequest(BaseModel, JsonBodyMarshaller):
    job_id: str


@blueprint.route('/bulk_action/upload_completed', methods=['POST'])
@require_ability(CanBulkAction)
@marshal_with(UploadCompletedRequest)
def upload_completed(upload_completed_request: UploadCompletedRequest) -> Any:
    if current_app.testing:
        return jsonify({'id': upload_completed_request.job_id}), 200
    # TODO(ayubun): Support bulk action service
    return abort(501, 'Not Implemented')


@blueprint.route('/bulk_action/upload/<job_id>/<file_name>', methods=['PUT', 'POST'])
@require_multipart_form_data
@require_ability(CanBulkAction)
def upload_file(job_id, file_name) -> Any:
    # TODO(ayubun): Support bulk action service
    return abort(501, 'Not Implemented')


@blueprint.route('/bulk_action/jobs', methods=['GET'])
@require_ability(CanBulkAction)
def get_jobs() -> Any:
    jobs = BulkActionJob.get_all()
    return {'jobs': [job.serialize() for job in jobs]}, 200


@blueprint.route('/bulk_action/jobs/<job_id>', methods=['GET'])
@require_ability(CanBulkAction)
def get_job(job_id: str) -> Any:
    job = BulkActionJob.get_one(int(job_id))
    if not job:
        return jsonify({'error': 'Job not found'}), 404

    return jsonify(job.serialize()), 200


@blueprint.route('/bulk_action/jobs/<job_id>/cancel', methods=['POST'])
@require_ability(CanBulkAction)
def cancel_job(job_id: str) -> Any:
    job = BulkActionJob.get_one(int(job_id))
    if not job:
        return jsonify({'error': 'Job not found'}), 404

    job.cancel()

    return jsonify({}), 200
