from typing import Any

from flask import Blueprint, abort, jsonify
from osprey.worker.lib.osprey_shared.labels import EntityLabelMutation
from osprey.worker.lib.singletons import LABELS_PROVIDER
from osprey.worker.ui_api.osprey.lib.abilities import (
    CanMutateEntities,
    CanMutateLabels,
    CanViewEventsByEntity,
    CanViewLabels,
    CanViewLabelsForEntity,
    require_ability,
    require_ability_with_request,
)
from osprey.worker.ui_api.osprey.lib.auth import get_current_user, get_current_user_email

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

    labels_provider = LABELS_PROVIDER.instance()
    if not labels_provider:
        return {
            'labels': {},
            # this field is deprecated
            'expires_at': None,
        }

    entity_labels = labels_provider.get_from_service(key=request_model.entity)
    #  Filter out all but the allowed labels
    ability = get_current_user().get_ability(CanViewLabels)

    response_labels = {}
    if hasattr(entity_labels, 'labels'):
        for label_name, label_state in entity_labels.labels.items():
            if ability and ability.item_is_allowed(label_name):
                response_labels[label_name] = label_state.serialize()

    return {
        'labels': response_labels,
    }


@blueprint.route('/entities/event-count-by-feature', methods=['POST'])
@marshal_with(EventCountsByFeatureForEntityQuery)
@require_ability(CanViewEventsByEntity)
def event_counts_by_feature_for_entity_query(request_model: EventCountsByFeatureForEntityQuery) -> Any:
    require_ability_with_request(request_model, CanViewEventsByEntity)
    timeseries_result = request_model.execute()
    return jsonify(timeseries_result[0]['result'])


@blueprint.route('/entities/labels', methods=['POST'])
@marshal_with(ManualEntityLabelMutationRequest)
def manual_entity_mutation(request_model: ManualEntityLabelMutationRequest) -> Any:
    require_ability_with_request(request_model, CanMutateEntities)
    require_ability_with_request(request_model, CanMutateLabels)

    labels_provider = LABELS_PROVIDER.instance()
    if not labels_provider:
        return abort(501, 'Labels Provider Not Found')

    can_mutate_labels_ability = get_current_user().get_ability(CanMutateLabels)
    # We can make this assertion because of the above line that requires CanMutateLabel for the request
    assert can_mutate_labels_ability is not None

    mutations: list[EntityLabelMutation] = []
    for request_mutation in request_model.mutations:
        if not can_mutate_labels_ability.item_is_allowed(request_mutation.label_name):
            continue
        entity_mutation = EntityLabelMutation(
            label_name=request_mutation.label_name,
            status=request_mutation.status,
            expires_at=request_mutation.expires_at,
            reason_name='_ManuallyUpdated',
            description='Manual update by {AdminEmail}: {Reason}',
            features={'AdminEmail': get_current_user_email(), 'Reason': request_mutation.reason},
        )
        mutations.append(entity_mutation)

    result = labels_provider.apply_entity_label_mutations(request_model.entity, mutations)

    return {
        'mutation_result': {
            'added': result.labels_added,
            'removed': result.labels_removed,
            'updated': result.labels_updated,
            'unchanged': list(set(mut.mutation.label_name for mut in result.dropped_mutations)),
        },
        **result.new_entity_labels.serialize(),
    }
