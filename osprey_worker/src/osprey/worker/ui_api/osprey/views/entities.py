from datetime import datetime
from typing import Any, Dict, Optional

from flask import Blueprint, abort, jsonify
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

    # TODO(ayubun): Support plug-and-play label service
    return {
        'labels': {},
        'expires_at': None,
    }

    # entity_labels = labels.get_for_entity(entity_key=request_model.entity.to_proto())
    # #  Filter out all but the allowed labels
    # ability = get_current_user().get_ability(CanViewLabels)

    # response_labels = {}
    # if hasattr(entity_labels, 'labels'):
    #     for label_name, label_state in entity_labels.labels.items():
    #         if ability and ability.item_is_allowed(label_name):
    #             response_labels[label_name] = MessageToDict(
    #                 label_state,
    #                 use_integers_for_enums=True,
    #                 preserving_proto_field_name=True,
    #             )

    # return {
    #     'labels': response_labels,
    #     'expires_at': entity_labels.expires_at.ToDatetime() if hasattr(entity_labels, 'expires_at') else None,
    # }


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

    # TODO(ayubun): Support plug-and-play label service
    return abort(501, 'Not Implemented')

    # can_mutate_labels_ability = get_current_user().get_ability(CanMutateLabels)
    # # We can make this assertion because of the above line that requires CanMutateLabel for the request
    # assert can_mutate_labels_ability is not None

    # mutations: List[ExtendedEntityMutation] = []
    # for request_mutation in request_model.mutations:
    #     if not can_mutate_labels_ability.item_is_allowed(request_mutation.label_name):
    #         continue
    #     entity_mutation = ExtendedEntityMutation(
    #         mutation=EntityMutation(
    #             label_name=request_mutation.label_name,
    #             status=request_mutation.status.value,
    #             expires_at=optional_datetime_to_timestamp(request_mutation.expires_at),
    #             reason_name='_ManuallyUpdated',
    #             description='Manual update by {AdminEmail}: {Reason}',
    #             features={'AdminEmail': get_current_user_email(), 'Reason': request_mutation.reason},
    #         ),
    #         delay_action_by=None,
    #     )
    #     mutations.append(entity_mutation)

    # # TODO Give unique ids to manual update requests.
    # mutation_result_external = EVENT_EFFECT_SINK.instance().apply_label_mutations_pb2(
    #     mutation_event_type=MutationEventType.MANUAL_UPDATE,
    #     mutation_event_id=get_current_user_email(),
    #     entity_key=request_model.entity.to_proto(),
    #     mutations=mutations,
    # )
    # mutation_result = ApplyEntityMutationReply.from_pb2(mutation_result_external)

    # entity_labels_internal = labels.get_for_entity(request_model.entity.to_proto())
    # entity_labels = LabelsModel.from_pb2(entity_labels_internal)
    # return EntityLabelMutationResult(
    #     labels=entity_labels.labels, expires_at=entity_labels.expires_at, mutation_result=mutation_result
    # )
