import copy
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from enum import Enum
from typing import TYPE_CHECKING, Dict, List, Mapping, Optional

from pydantic import BaseModel

from osprey.engine.language_types.labels import LabelStatus
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.lib.utils.request_utils import SessionWithRetries

if TYPE_CHECKING:
    pass


# The requests session we will be using to contact osprey API.
_session = SessionWithRetries()

_REQUEST_TIMEOUT_SECS = 5


logger = get_logger(__name__)


#  If you change this also change osprey/osprey_engine/packages/osprey_stdlib/configs/labels_config.py
class LabelConnotation(Enum):
    POSITIVE = 'positive'
    NEGATIVE = 'negative'
    NEUTRAL = 'neutral'


@dataclass
class LabelReason:
    """
    a label reason tells us why a label mutation was made, when it happened, and when it expires (if at all)
    """

    pending: bool = False
    description: str = ''
    """why the label was mutated"""
    features: dict[str, str] = field(default_factory=dict)
    """features are injected into the description as k/v's, similar to how fstrings work. for example, 
    the {you} in 'hello {you}' would be substituted as 'person' with a feature dict of {'you': 'person'}"""
    created_at: datetime | None = None
    """
    when this reason was made
    """
    expires_at: datetime | None = None
    """marks when this label reason 'expires'

    if a LabelState.MANUALLY_REMOVED is applied with a reason that has a 1 day expiration, then 
    for 1 day, the label cannot be applied via LabelState.ADDED. all LabelState.ADDED attempts will be dropped.

    if a given label state has multiple label reasons, all reasons would need to expire before the status/state
    is considered expired, too. 
    """

    def is_expired(self) -> bool:
        return bool(self.expires_at is not None and self.expires_at + timedelta(seconds=1) < datetime.now())


@dataclass
class LabelStateInner:
    status: LabelStatus
    reasons: Dict[str, LabelReason]


@dataclass
class LabelState:
    status: LabelStatus
    """statuses dictate the way the current state behaves; certain statuses have priority over others 
    (see LabelStatus for more info)"""

    reasons: dict[str, LabelReason]
    """
    reasons are why this label state was applied; it is a dict because there may be multiple,
    with each reason being distinct based on it's reason name.

    reasons applied under the same name are merged, with precedence given to newer creaeted_at timestamps.
    """

    previous_states: List[LabelStateInner] = field(default_factory=list)
    """the top-level label state also contains previous label states; we use an inner type
    because we don't need these prior states to have the previous_states field"""

    @property
    def expires_at(self) -> datetime | None:
        """
        when a given label state is effectively expired. expiration can only occur if all of the
        reasons are expired.

        this field is a convenience value to save users time on computing the effective expiration time from the reasons.

        expiration defines when future label states can be applied. if the current label state is not expired,
        then then upon a new label state change attempt, the current and new statuses have their weights' compared.
        whichever has the higher weight will take precedence, and the lower weight(s) will be dropped.
        if the weights are the *same*, then a merge of reasons is performed, which can also cause the expiration to be delayed.
        """
        if not self.reasons:
            AssertionError(f'invariant: the label state {self} did not have any associated reasons')
        expires_at = datetime.min
        for reason in self.reasons.values():
            if reason.expires_at is None:
                return None
            expires_at = max(reason.expires_at, expires_at)
        return expires_at

    def is_expired(self) -> bool:
        return bool(self.expires_at is not None and self.expires_at + timedelta(seconds=1) < datetime.now())

    def _shift_current_state_to_previous_state(self) -> None:
        if not self.reasons:
            # to make this function idempotent, we don't want to shift an empty state to the previous state.
            # we should always have reasons to shift
            return
        self.previous_states.insert(0, LabelStateInner(status=self.status, reasons=copy.deepcopy(self.reasons)))
        self.reasons.clear()

    def update_status(self, status: LabelStatus, reasons: dict[str, LabelReason]) -> bool:
        """
        sets the label state to the provided status if the provided status has a higher weight or if the current
        status is expired.

        if the status is updated, the current state is shifted to previous_states and the reasons are appended.

        returns true if the status is accepted and/or merged; returns false if it was dropped.
        """
        if self.is_expired() or self.status.weight < status.weight:
            # if the curr state is expired, we will shift those reasons to prev state and make the new reason the curr state.
            # append_reason will automatically do this for us at the start of the code. we just need to update the status
            self._shift_current_state_to_previous_state()
            self.status = status
            self.reasons = reasons
            return True

        if self.status.weight > status.weight:
            # if our current weight is alr higher and non-expired, the new status is droppable
            return False

        # if we made it here, the statuses are equal weight
        if self.status != status:
            self._shift_current_state_to_previous_state()
            self.status = status
            self.reasons = reasons
            return True

        # if statuses are the same, and a currently non-expired reason exists (meaning the state as a whole is unexpired),
        # we will simply attempt to append the new reasons to the existing reason(s)~
        # # this would only fail if the creation time of the new reason is older than the existing reason(s), which shouldn't happen(?)
        success = all(self.append_reason(reason_name, reason) for reason_name, reason in reasons.items())
        if not success:
            logger.error(f'update_status could not append reasons {reasons} to state {self}')
        return True

    def append_reason(self, reason_name: str, reason: LabelReason) -> bool:
        """
        returns true if the reason was able to be appended and/or merged with an existing reason;
        false if it was dropped due to being older than the current reason
        """
        if self.is_expired():
            self._shift_current_state_to_previous_state()

        if reason_name not in self.reasons:
            self.reasons[reason_name] = reason
            return True

        current_reason = self.reasons[reason_name]
        if current_reason.created_at is None or reason.created_at is None:
            raise AssertionError(
                f'invariant: missing created_at on one of the following LabelReasons: {current_reason} {reason}'
            )

        if current_reason.created_at > reason.created_at + timedelta(seconds=1):
            # the reason we are trying to append is older, so we will discard it (1sec added to adjust for potential code exec time)
            return False

        self.reasons[reason_name] = replace(
            reason,
            created_at=current_reason.created_at,
        )
        return True


@dataclass
class EntityLabels:
    """this class represents a given entity's current labels & label states"""

    labels: Dict[str, LabelState] = field(default_factory=dict)
    """a mapping of label names to their current states'"""


class LabelsAndConnotationsResponse(BaseModel):
    labels: EntityLabels
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
class EntityLabelMutation:
    """
    a class that allows callers of LabelsProvider.apply_entity_label_mutations() to request how an
    entity's labels should be mutated.

    mutations are not guaranteed to be written to the labels provider. see EntityLabelMutationsResult.dropped
    """

    label_name: str = ''
    reason_name: str = ''
    status: LabelStatus = LabelStatus.ADDED
    pending: bool = False
    description: str = ''
    features: dict[str, str] = field(default_factory=dict)
    expires_at: datetime | None = None
    delay_action_by: timedelta | None = None
    """
    in the event that this mutation is successfully applied to an entity, an action may occur via the
    after_add or after_remove LabelsService definitions.

    if the LabelAdd or LabelRemove call that created this mutation specified a delay_action_by, then the
    post-label action will be delayed by said timedelta, assuming that no shutdown/stop signal forces a
    more imminent execution.
    """

    def desired_state(self) -> LabelState:
        return LabelState(
            status=self.status,
            reasons={self.reason_name: self.reason()},
        )

    @property
    def reason(self) -> LabelReason:
        return LabelReason(
            pending=self.pending,
            description=self.description,
            features=self.features,
            created_at=datetime.now(),
            expires_at=self.expires_at,
        )

@dataclass
class EntityLabelMutationsResult:
    new_entity_labels: EntityLabels
    """
    all of the entity's labels post-mutation
    """

    old_entity_labels: Optional[EntityLabels] = None
    """
    all of the entity's labels pre-mutation
    """

    added: list[str] = field(default_factory=list)
    """
    all (effective-status) label adds that occurred during this mutation
    """

    removed: list[str] = field(default_factory=list)
    """
    all (effective-status) label removes that occurred during this mutation
    """

    updated: list[str] = field(default_factory=list)
    """
    labels that had their state updated. this can include simply updating the reason.
    """

    dropped: list[EntityLabelMutation] = field(default_factory=list)
    """
    dropped mutations occur when the current label state is unexpired and has a label
    status with a higher weight than the mutation attempts to make.

    dropping can also happen if there are errors during the mutations.
    """


class EntityLabelDisagreeResponse(BaseModel):
    mutation_result: EntityLabelMutationsResult
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
