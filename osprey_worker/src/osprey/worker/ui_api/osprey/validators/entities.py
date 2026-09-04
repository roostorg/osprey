from datetime import datetime
from typing import Any

from flask import Request
from osprey.engine.language_types.entities import EntityT
from osprey.worker.lib.osprey_shared.labels import LabelStatus
from osprey.worker.ui_api.osprey.lib.druid import TimeseriesDruidQuery
from osprey.worker.ui_api.osprey.lib.marshal import JsonBodyMarshaller
from pydantic import BaseModel


class EntityMarshaller(JsonBodyMarshaller):
    """The entity is addressed by query string; everything else comes from the body.

    Only the reshaping of `entity_id` and `entity_type` into the nested `entity` field
    is this marshaller's own business -- the body is read by `JsonBodyMarshaller`, which
    is why this no longer has to know what a body is. When it did, it read one body key
    at a time from `get_json()` and spread the result: an unlabelled body was dropped in
    silence, and a body that parsed to a list raised `TypeError` on the spread, turning
    a two-character request into a 500.
    """

    @classmethod
    def overrides(cls, flask_request: Request) -> dict[str, Any]:
        return {'entity': {'id': flask_request.args['entity_id'], 'type': flask_request.args['entity_type']}}


class GetLabelsForEntityRequest(BaseModel, EntityMarshaller):
    entity: EntityT[str]


class EventCountsByFeatureForEntityQuery(TimeseriesDruidQuery, EntityMarshaller):
    pass


class EntityLabelMutation(BaseModel):
    label_name: str
    status: LabelStatus
    reason: str
    expires_at: datetime | None


class ManualEntityLabelMutationRequest(BaseModel, EntityMarshaller):
    entity: EntityT[str]
    mutations: list[EntityLabelMutation]
