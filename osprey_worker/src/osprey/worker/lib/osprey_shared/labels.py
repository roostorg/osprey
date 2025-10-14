import copy
from collections import UserDict
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from enum import Enum
from typing import TYPE_CHECKING, Dict, List, Mapping, Optional

from osprey.engine.language_types.labels import LabelStatus
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.lib.utils.request_utils import SessionWithRetries
from pydantic import BaseModel

if TYPE_CHECKING:
    pass


# The requests session we will be using to contact osprey API.
_session = SessionWithRetries()

_REQUEST_TIMEOUT_SECS = 5


logger = get_logger(__name__)


class MutationDropReason(IntEnum):
    # If a label mutation was dropped due to another mutation that conflicted & was higher priority
    # (priority of conflicting mutations in a given entity update is determined by the int value of the
    # label status enum)
    CONFLICTING_MUTATION = 0
    # If the existing label status was manual and the attempted mutation was not
    CANNOT_OVERRIDE_MANUAL = 1


class LabelStatus(IntEnum):
    """
    indicates the status of label.

    regular (a.k.a. "automatic") statuses are applied via rules. they can be overwritten by manual
    statuses, which can only be applied via humans using the ui.

    statuses have weights, which control which ones get dropped when conflicting statuses occur during
    a single attempted mutation; i.e., if an execution of the rules results in a label add and a label remove
    of the same entity/label pair.
    """

    ADDED = 0
    REMOVED = 1
    MANUALLY_ADDED = 2
    MANUALLY_REMOVED = 3

    def effective_label_status(self) -> 'LabelStatus':
        """
        Returns the effective status of the label, which is what the upstreams that are observing label
        status changes will see. Which is to say, the upstreams will currently not see if the label status was
        manually added or manually removed, just that it was added or removed.
        """
        match self:
            case LabelStatus.ADDED | LabelStatus.MANUALLY_ADDED:
                return LabelStatus.ADDED
            case LabelStatus.REMOVED | LabelStatus.MANUALLY_REMOVED:
                return LabelStatus.REMOVED

    def is_manual(self) -> bool:
        match self:
            case LabelStatus.MANUALLY_ADDED | LabelStatus.MANUALLY_REMOVED:
                return True
            case _:
                return False

    def is_automatic(self) -> bool:
        return not self.is_manual()


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
        return bool(self.expires_at is not None and self.expires_at + timedelta(seconds=5) < datetime.now())


class LabelReasons(UserDict[str, LabelReason]):
    """
    the label reasons userdict allows us to add a helper function to the dict directly, while otherwise
    operating as a normal dict would~
    """

    def insert_or_update(self, reason_name: str, reason: LabelReason) -> bool:
        """
        returns true if the reason was able to be inserted or updated an existing reason;
        false if it was dropped due to being older than the current reason
        """
        if reason_name not in self:
            self[reason_name] = reason
            return True

        current_reason = self[reason_name]
        if current_reason.created_at is None or reason.created_at is None:
            raise AssertionError(
                f'invariant: missing created_at on one of the following LabelReasons: {current_reason} {reason}'
            )

        if current_reason.created_at > reason.created_at + timedelta(seconds=5):
            # the reason we are trying to append is older than the one currently at the reason_name key,
            # so we will discard it (5sec added to adjust for potential code exec time).
            return False

        self[reason_name] = replace(
            reason,
            # since the current reason is older by this point in the code, we want to preserve the original created_at timestamp
            created_at=current_reason.created_at,
        )
        return True


@dataclass
class LabelStateInner:
    status: LabelStatus
    reasons: LabelReasons

    def into_outer(self) -> LabelState:
        return LabelState(
            status=self.status,
            reasons=self.reasons,
        )


@dataclass
class LabelState:
    status: LabelStatus
    """statuses dictate the way the current state behaves; certain statuses have priority over others 
    (see LabelStatus for more info)"""

    reasons: LabelReasons
    """
    reasons are why this label state was applied; it is a dict because there may be multiple,
    with each reason being distinct based on it's reason name.

    reasons applied under the same name are merged (assuming the status has not changed), 
    with precedence given to newer creaeted_at timestamps.
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
        return bool(self.expires_at is not None and self.expires_at + timedelta(seconds=5) < datetime.now())

    def _shift_current_state_to_previous_state(self) -> None:
        if not self.reasons:
            # to make this function idempotent, we don't want to shift an empty state to the previous state.
            # we should always have reasons to shift
            return
        self.previous_states.insert(
            0, LabelStateInner(status=copy.copy(self.status), reasons=copy.deepcopy(self.reasons))
        )
        self.reasons = dict()

    def try_apply_desired_state(self, desired_state: LabelStateInner) -> MutationDropReason | None:
        """
        attempts to apply the desired state to this state.
        if the state could not be applied (i.e. due to an unexpired manual status blocking
        a status change to an automatic status), this method will return the MutationDropReason that
        should be applied to the responsible mutations. otherwise, it will return None to indicate success
        """
        if self.is_expired():
            self._shift_current_state_to_previous_state()
            self.status = desired_state.status
            self.reasons = desired_state.reasons
            return None

        # if the current status is manual, we will drop automatic statuses (unless the current state is expired)
        if self.status.is_manual() and desired_state.status.is_automatic():
            return MutationDropReason.CANNOT_OVERRIDE_MANUAL

        # if the statuses are different and we've made it this far, the desired state is allowed to overwrite
        # the current state. so lets do that by shifting to previous state and updating
        if self.status != desired_state.status:
            self._shift_current_state_to_previous_state()
            self.status = desired_state.status

        for reason_name, reason in desired_state.reasons.items():
            self.reasons.insert_or_update(reason_name, reason)

        return None


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

    def desired_state(self) -> LabelStateInner:
        return LabelStateInner(
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
class DroppedEntityLabelMutation:
    mutation: EntityLabelMutation
    reason: MutationDropReason


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

    labels_added: list[str] = field(default_factory=list)
    """
    all (effective-status) label adds that occurred during this mutation
    """

    labels_removed: list[str] = field(default_factory=list)
    """
    all (effective-status) label removes that occurred during this mutation
    """

    labels_updated: list[str] = field(default_factory=list)
    """
    labels that had their state updated. this can include simply updating or 
    appending to the reason
    """

    dropped_mutations: list[DroppedEntityLabelMutation] = field(default_factory=list)
    """
    mutations that were dropped for one reason or another. each dropped mutation is
    given a drop reason
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
