from typing import Any

from flask import Blueprint, abort, jsonify
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
#               I have modified the endpoints to return 501 for now.


@blueprint.route('/bulk_action/start', methods=['POST'])
@require_ability(CanBulkAction)
@marshal_with(StartBulkActionJobRequest)
def start_bulk_job(start_bulk_action_job_request: StartBulkActionJobRequest) -> Any:
    # TODO(ayubun): Support bulk action service
    return abort(501, 'Not Implemented')

    # file_manager: BulkActionFileManager = current_app.bulk_action_file_manager

    # job_id = generate_snowflake().to_int()
    # BulkActionJob.create_job(
    #     job_id=job_id,
    #     user_id=get_current_user_email(),
    #     gcs_path=f'{job_id}/{start_bulk_action_job_request.file_name}',
    #     original_filename=start_bulk_action_job_request.file_name,
    #     total_rows=0,
    #     action_workflow_name=start_bulk_action_job_request.workflow_name,
    #     entity_type=start_bulk_action_job_request.entity_type,
    #     name=start_bulk_action_job_request.job_name,
    #     description=start_bulk_action_job_request.job_description,
    # )

    # url = file_manager.generate_upload_url(f'{job_id}/{start_bulk_action_job_request.file_name}')
    # return (
    #     jsonify(
    #         {
    #             'id': str(job_id),
    #             'url': url,
    #         }
    #     ),
    #     200,
    # )


class UploadCompletedRequest(BaseModel, JsonBodyMarshaller):
    job_id: str


@blueprint.route('/bulk_action/upload_completed', methods=['POST'])
@require_ability(CanBulkAction)
@marshal_with(UploadCompletedRequest)
def upload_completed(upload_completed_request: UploadCompletedRequest) -> Any:
    # TODO(ayubun): Support bulk action service
    return abort(501, 'Not Implemented')

    # job = BulkActionJob.get_one(int(upload_completed_request.job_id))
    # if not job:
    #     return jsonify({'error': 'Job not found'}), 404

    # job.update_job(status=BulkActionJobStatus.UPLOADED)

    # return (
    #     jsonify(
    #         {
    #             'id': upload_completed_request.job_id,
    #         }
    #     ),
    #     200,
    # )


@blueprint.route('/bulk_action/upload/<job_id>/<file_name>', methods=['PUT', 'POST'])
@require_multipart_form_data
@require_ability(CanBulkAction)
def upload_file(job_id, file_name) -> Any:
    # TODO(ayubun): Support bulk action service
    return abort(501, 'Not Implemented')

    # file = request.files.get('file')
    # if file is None or len(request.files) != 1:
    #     return jsonify({'error': 'Invalid file'}), 400

    # file_manager: BulkActionFileManager = current_app.bulk_action_file_manager
    # file_manager.upload_file(f'{job_id}/{file_name}', file)
    # return jsonify({}), 200


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
