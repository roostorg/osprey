from abc import ABC
from datetime import timedelta
from typing import Any, List, Optional, Sequence, Union

from osprey.engine.executor.external_service_utils import ExternalService
from osprey.engine.language_types.entities import EntityT
from osprey.rpc.labels.v1 import service_pb2 as pb2
from osprey.rpc.labels.v1.service_pb2 import LabelStatus
from osprey.rpc.labels.v1.service_pb2_grpc import LabelServiceStub
from osprey.worker.lib.pigeon.client import RoutedClient, RoutingType
from osprey.worker.lib.utils.grpc import DATA_SERVICES_RETRY_POLICY
from osprey.worker.lib.utils.grpc_client_pool import GrpcClientPool
from result import Err, Ok, Result

_grpc_client_pool = GrpcClientPool(
    # pls do profiling if you wish to modify this size!
    size=1,
    func=lambda seq: RoutedClient(
        'osprey_labels',
        stub_cls=LabelServiceStub,
        request_field='routing_key',
        secondaries=2,
        routing_type=RoutingType.SCALAR,
        read_timeout=0.6,
        acceptable_duration_ms=40,
        grpc_options={'grpc.http2.initial_sequence_number': seq},
    ),
)


def get_label_routing_key(entity_key: Union[pb2.EntityKey, EntityT[Any]]) -> str:
    if isinstance(entity_key, pb2.EntityKey):
        return entity_key.type + '/' + entity_key.id
    return entity_key.type + '/' + str(entity_key.id)


def get_for_entity(entity_key: pb2.EntityKey) -> pb2.Labels:
    request = pb2.GetEntityRequest(routing_key=get_label_routing_key(entity_key), key=entity_key)
    response: pb2.GetEntityResponse = _grpc_client_pool.get().GetEntity(
        request, retry_policy=DATA_SERVICES_RETRY_POLICY
    )
    return response.entity.labels


def batch_get_for_entity(entity_keys: Sequence[pb2.EntityKey]) -> Sequence[Result[pb2.Labels, Exception]]:
    request = pb2.GetEntityBatchRequest(routing_key=get_label_routing_key(entity_keys[0]), keys=list(entity_keys))
    response: pb2.GetEntityBatchResponse = _grpc_client_pool.get().GetEntityBatch(
        request, retry_policy=DATA_SERVICES_RETRY_POLICY
    )
    return [
        Ok(response.entity.labels) if response.HasField('entity') else Err(KeyError(entity_keys[i]))
        for i, response in enumerate(response.responses)
    ]


def apply_entity_mutation(
    entity_key: pb2.EntityKey, mutations: List[pb2.EntityMutation]
) -> pb2.ApplyEntityMutationReply:
    request = pb2.ApplyEntityMutationRequest(
        routing_key=get_label_routing_key(entity_key), key=entity_key, mutations=mutations
    )
    response: pb2.ApplyEntityMutationReply = _grpc_client_pool.get().ApplyEntityMutation(
        request, retry_policy=DATA_SERVICES_RETRY_POLICY
    )
    return response


def get_effective_label_status(label_status: LabelStatus.ValueType) -> LabelStatus.ValueType:
    """
    Returns the effective status of the label, which is what the upstreams that are observing label
    status changes will see. Which is to say, the upstreams will currently not see if the label status was
    manually added or manually removed, just that it was added or removed.
    """

    if label_status in (pb2.LabelStatus.ADDED, pb2.LabelStatus.MANUALLY_ADDED):
        return pb2.LabelStatus.ADDED
    elif label_status in (pb2.LabelStatus.REMOVED, pb2.LabelStatus.MANUALLY_REMOVED):
        return pb2.LabelStatus.REMOVED

    raise ValueError(f'Unexpected LabelStatus: {label_status!r}')


class LabelProvider(ExternalService[EntityT[Any], pb2.Labels], ABC):
    def cache_ttl(self) -> Optional[timedelta]:
        return timedelta(minutes=5)


class HasLabelProvider(LabelProvider):
    def get_from_service(self, key: EntityT[Any]) -> pb2.Labels:
        return get_for_entity(pb2.EntityKey(type=key.type, id=str(key.id)))

    def batch_get_from_service(self, keys: Sequence[EntityT[Any]]) -> Sequence[Result[pb2.Labels, Exception]]:
        return batch_get_for_entity([pb2.EntityKey(type=key.type, id=str(key.id)) for key in keys])
