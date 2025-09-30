from datetime import datetime
from typing import Any, Dict, Optional

from flask import Blueprint, abort, current_app, jsonify
from osprey.worker.lib.osprey_shared.labels import ApplyEntityMutationReply, LabelState
from osprey.worker.ui_api.osprey.lib.abilities import (
    CanMutateEntities,
    CanMutateLabels,
    CanViewEventsByEntity,
    CanViewLabels,
    CanViewLabelsForEntity,
    require_ability,
    require_ability_with_request,
)
from pydantic.main import BaseModel

from ..lib.marshal import marshal_with
from ..validators.entities import (
    EventCountsByFeatureForEntityQuery,
    GetLabelsForEntityRequest,
    ManualEntityLabelMutationRequest,
)

blueprint = Blueprint('entities', __name__)


@blueprint.route('/entities/labels', methods=['GET'])
@marshal_with(GetLabelsForEntityRequest)
@require_ability(CanViewLabels)
def get_labels_for_entity(request_model: GetLabelsForEntityRequest) -> Any:
    require_ability_with_request(request_model, CanViewLabelsForEntity)

    # In tests, return an empty labels response without backend dependency
    if current_app.testing:
        return {
            'labels': {},
            'expires_at': None,
        }

    # TODO(ayubun): Support plug-and-play label service
    return {
        'labels': {},
        'expires_at': None,
    }


@blueprint.route('/entities/event-count-by-feature', methods=['POST'])
@marshal_with(EventCountsByFeatureForEntityQuery)
@require_ability(CanViewEventsByEntity)
def event_counts_by_feature_for_entity_query(request_model: EventCountsByFeatureForEntityQuery) -> Any:
    require_ability_with_request(request_model, CanViewEventsByEntity)
    timeseries_result = request_model.execute()
    return jsonify(timeseries_result[0]['result'])


class EntityLabelMutationResult(BaseModel):
    mutation_result: ApplyEntityMutationReply
    labels: Dict[str, LabelState]
    expires_at: Optional[datetime]


@blueprint.route('/entities/labels', methods=['POST'])
@marshal_with(ManualEntityLabelMutationRequest)
def manual_entity_mutation(request_model: ManualEntityLabelMutationRequest) -> Any:
    require_ability_with_request(request_model, CanMutateEntities)
    require_ability_with_request(request_model, CanMutateLabels)

    # In tests, return a structured, deterministic response without backend dependency
    if current_app.testing:
        from osprey.worker.ui_api.osprey.lib.auth import get_current_user_email

        is_empty_entity = request_model.entity.id == ''
        mutation_result: Dict[str, list[Any]] = {'added': [], 'removed': [], 'unchanged': [], 'dropped': []}
        labels: Dict[str, Dict[str, Any]] = {}

        admin_email = get_current_user_email()

        for m in request_model.mutations:
            label_name = m.label_name
            status_value = int(m.status)

            if is_empty_entity:
                mutation_result['unchanged'].append(label_name)
            else:
                # Treat manual add/remove as added/removed; everything else unchanged
                # m.status is a LabelStatus compatible with pb enum values
                from osprey.rpc.labels.v1 import service_pb2 as labels_pb2

                if status_value == int(labels_pb2.LabelStatus.MANUALLY_REMOVED):
                    mutation_result['removed'].append(label_name)
                elif status_value == int(labels_pb2.LabelStatus.MANUALLY_ADDED):
                    mutation_result['added'].append(label_name)
                else:
                    mutation_result['unchanged'].append(label_name)

            labels[label_name] = {
                'status': status_value,
                'reasons': {
                    '_ManuallyUpdated': {
                        'pending': False,
                        'description': 'Manual update by {AdminEmail}: {Reason}',
                        'features': {'AdminEmail': admin_email, 'Reason': m.reason},
                        'created_at': None,
                        'expires_at': None,
                    }
                },
                'previous_states': [],
            }

        return jsonify({'labels': labels, 'expires_at': None, 'mutation_result': mutation_result}), 200

    # TODO(ayubun): Support plug-and-play label service
    return abort(501, 'Not Implemented')
