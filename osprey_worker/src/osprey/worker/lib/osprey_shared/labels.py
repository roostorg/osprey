from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Dict, List, Mapping, Optional

from osprey.engine.language_types.labels import LabelStatus
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


@dataclass
class ApplyEntityMutationReply:
    added: List[str] = field(default_factory=list)
    removed: List[str] = field(default_factory=list)
    unchanged: List[str] = field(default_factory=list)
    dropped: List[EntityMutation] = field(default_factory=list)


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
