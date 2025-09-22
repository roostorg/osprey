from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Type

from flask import Request
from osprey.engine.language_types.entities import EntityT
from osprey.engine.language_types.labels import LabelStatus
from osprey.rpc.labels.v1 import service_pb2
from osprey.worker.ui_api.osprey.lib.druid import TimeseriesDruidQuery
from osprey.worker.ui_api.osprey.lib.marshal import FlaskRequestMarshaller, T
from pydantic import BaseModel


# This type exists in addition to the pb2 one because pb2 EntityKey cannot be
# used in pydantic models
@dataclass(frozen=True)
class EntityKey(EntityT[str]):
    id: str
    type: str

    @classmethod
    def from_proto(cls, proto: service_pb2.EntityKey):
        return cls(id=proto.id, type=proto.type)

    def to_proto(self) -> service_pb2.EntityKey:
        return service_pb2.EntityKey(id=self.id, type=self.type)


class EntityMarshaller(FlaskRequestMarshaller):
    @classmethod
    def marshal(cls: Type[T], flask_request: Request) -> T:
        body = flask_request.get_json()
        entity = {'entity': {'id': flask_request.args['entity_id'], 'type': flask_request.args['entity_type']}}
        if not body:
            return cls.parse_obj(entity)
        return cls.parse_obj({**body, **entity})


class GetLabelsForEntityRequest(BaseModel, EntityMarshaller):
    entity: EntityKey


class EventCountsByFeatureForEntityQuery(TimeseriesDruidQuery, EntityMarshaller):
    pass


class EntityLabelMutation(BaseModel):
    label_name: str
    status: LabelStatus
    reason: str
    expires_at: Optional[datetime]


class ManualEntityLabelMutationRequest(BaseModel, EntityMarshaller):
    entity: EntityKey
    mutations: List[EntityLabelMutation]
