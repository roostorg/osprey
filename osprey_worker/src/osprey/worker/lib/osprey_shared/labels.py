from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, IntEnum
from typing import TYPE_CHECKING, Dict, List, Mapping, Optional, cast

from osprey.engine.language_types.labels import LabelStatus
from osprey.rpc.labels.v1 import service_pb2
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.lib.utils.request_utils import SessionWithRetries
from pydantic import BaseModel

if TYPE_CHECKING:
    from osprey.worker.lib.utils.flask_signing import Signer


# The requests session we will be using to contact osprey API.
_session = SessionWithRetries()

_REQUEST_TIMEOUT_SECS = 5


logger = get_logger(__name__)


#  If you change this also change osprey/osprey_engine/packages/osprey_stdlib/configs/labels_config.py
class LabelConnotation(Enum):
    POSITIVE = 'positive'
    NEGATIVE = 'negative'
    NEUTRAL = 'neutral'

# Pydantic-compatible versions of pb2 types
@dataclass
class LabelReason:
    pending: bool = False
    description: str = ''
    features: Dict[str, str] = field(default_factory=dict)
    created_at: datetime | None = None
    expires_at: datetime | None = None

    @classmethod
    def from_pb2(cls, pb2_reason: service_pb2.LabelReason) -> 'LabelReason':
        """Convert from pb2 LabelReason to dataclass."""
        created_at = None
        if pb2_reason.HasField('created_at'):
            created_at = pb2_reason.created_at.ToDatetime()

        expires_at = None
        if pb2_reason.HasField('expires_at'):
            expires_at = pb2_reason.expires_at.ToDatetime()

        return cls(
            pending=pb2_reason.pending,
            description=pb2_reason.description,
            features=dict(pb2_reason.features),
            created_at=created_at,
            expires_at=expires_at,
        )

    def to_pb2(self) -> service_pb2.LabelReason:
        """Convert to pb2 LabelReason."""
        pb2_reason = service_pb2.LabelReason(
            pending=self.pending,
            description=self.description,
            features=self.features,
        )

        if self.created_at is not None:
            pb2_reason.created_at.FromDatetime(self.created_at)
        if self.expires_at is not None:
            pb2_reason.expires_at.FromDatetime(self.expires_at)

        return pb2_reason


@dataclass
class LabelStateInner:
    status: LabelStatus
    reasons: Dict[str, LabelReason]


@dataclass
class LabelState:
    status: LabelStatus
    reasons: Dict[str, LabelReason]
    previous_states: List[LabelStateInner] = field(default_factory=list)


@dataclass
class Labels:
    labels: Dict[str, LabelState] = field(default_factory=dict)
    expires_at: Optional[datetime] = None


class LabelsAndConnotationsResponse(BaseModel):
    labels: Labels
    label_connotations: Mapping[str, LabelConnotation]


def get_labels_for_entity(
    endpoint: str, signer: 'Signer', entity_type: str, entity_id: str
) -> LabelsAndConnotationsResponse:
    url = f'{endpoint}entity/{entity_type}/{entity_id}/labels'
    headers = signer.sign_url(url)
    raw_resp = _session.get(url, headers=headers, timeout=_REQUEST_TIMEOUT_SECS)
    logger.info(f'[get_labels_for_entity] status code is {raw_resp.status_code}')
    raw_resp.raise_for_status()
    return LabelsAndConnotationsResponse.parse_obj(raw_resp.json())


class EntityLabelDisagreeRequest(BaseModel):
    label_name: str
    description: str
    admin_email: str
    expires_at: Optional[datetime]


@dataclass
class EntityMutation:
    label_name: str = ''
    reason_name: str = ''
    status: int = 0
    pending: bool = False
    description: str = ''
    features: Dict[str, 'str'] = field(default_factory=dict)
    expires_at: Optional[datetime] = None

    @classmethod
    def from_pb2(cls, pb2_mutation: service_pb2.EntityMutation) -> 'EntityMutation':
        """Convert from pb2 EntityMutation to dataclass."""
        expires_at = None
        if pb2_mutation.HasField('expires_at'):
            expires_at = pb2_mutation.expires_at.ToDatetime()

        return cls(
            label_name=pb2_mutation.label_name,
            reason_name=pb2_mutation.reason_name,
            status=pb2_mutation.status,
            pending=pb2_mutation.pending,
            description=pb2_mutation.description,
            features=dict(pb2_mutation.features),
            expires_at=expires_at,
        )

    def to_pb2(self) -> service_pb2.EntityMutation:
        """Convert to pb2 EntityMutation."""
        pb2_mutation = service_pb2.EntityMutation(
            label_name=self.label_name,
            reason_name=self.reason_name,
            status=cast('service_pb2.LabelStatus.ValueType', self.status),
            pending=self.pending,
            description=self.description,
            features=self.features,
        )

        if self.expires_at is not None:
            pb2_mutation.expires_at.FromDatetime(self.expires_at)

        return pb2_mutation


@dataclass
class ApplyEntityMutationReply:
    added: List[str] = field(default_factory=list)
    removed: List[str] = field(default_factory=list)
    unchanged: List[str] = field(default_factory=list)
    dropped: List[EntityMutation] = field(default_factory=list)

    @classmethod
    def from_pb2(cls, pb2_reply: service_pb2.ApplyEntityMutationReply) -> 'ApplyEntityMutationReply':
        """Convert from pb2 ApplyEntityMutationReply to dataclass."""
        return cls(
            added=list(pb2_reply.added),
            removed=list(pb2_reply.removed),
            unchanged=list(pb2_reply.unchanged),
            dropped=[EntityMutation.from_pb2(mutation) for mutation in pb2_reply.dropped],
        )

    def to_pb2(self) -> service_pb2.ApplyEntityMutationReply:
        """Convert to pb2 ApplyEntityMutationReply."""
        pb2_reply = service_pb2.ApplyEntityMutationReply()
        pb2_reply.added.extend(self.added)
        pb2_reply.removed.extend(self.removed)
        pb2_reply.unchanged.extend(self.unchanged)
        for mutation in self.dropped:
            pb2_reply.dropped.append(mutation.to_pb2())
        return pb2_reply


class EntityLabelDisagreeResponse(BaseModel):
    mutation_result: ApplyEntityMutationReply
    labels: Dict[str, LabelState]
    expires_at: Optional[datetime]


def disagree_wth_label(
    endpoint: str, signer: 'Signer', entity_type: str, entity_id: str, label_disagreement: EntityLabelDisagreeRequest
) -> EntityLabelDisagreeResponse:
    url = f'{endpoint}entity/{entity_type}/{entity_id}/labels/disagree'

    label_disagreement_bytes = label_disagreement.json().encode()
    headers = signer.sign(label_disagreement_bytes)

    raw_resp = _session.post(url, headers=headers, data=label_disagreement_bytes, timeout=_REQUEST_TIMEOUT_SECS)
    raw_resp.raise_for_status()
    return EntityLabelDisagreeResponse.parse_obj(raw_resp.json())
