from datetime import datetime
from typing import Type

from flask import Request
from osprey.engine.language_types.entities import EntityT
from osprey.worker.lib.osprey_shared.labels import LabelStatus
from osprey.worker.ui_api.osprey.lib.druid import TimeseriesDruidQuery
from osprey.worker.ui_api.osprey.lib.marshal import FlaskRequestMarshaller, T
from pydantic import BaseModel


class EntityMarshaller(FlaskRequestMarshaller):
    @classmethod
    def marshal(cls: Type[T], flask_request: Request) -> T:
        body = flask_request.get_json()
        entity = {'entity': {'id': flask_request.args['entity_id'], 'type': flask_request.args['entity_type']}}
        if not body:
            return cls.parse_obj(entity)
        return cls.parse_obj({**body, **entity})


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
