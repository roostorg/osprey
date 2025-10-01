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


@dataclass
class LabelReason:
    # Pendinding is generally unused, but kept for legacy.
    pending: bool = False
    description: str = ''
    features: Dict[str, str] = field(default_factory=dict)
    created_at: datetime | None = None
    expires_at: datetime | None = None

    def update_with_mutation(self, mutation: 'EntityMutation') -> 'LabelReason':
        """
        Updates this reason with an EntityMutation using expiration-aware logic.

        Implements reason-level expiration logic from label_provider_spec.md:
        - **Expired reason replacement**: Expired reasons get completely replaced, not updated
        - **Expiration-only updates**: Non-expired reasons with same content only update expires_at
        - **Content change handling**: Any content change creates new reason regardless of expiration

        Args:
            mutation: EntityMutation to apply to this reason

        Returns:
            LabelReason: Updated reason (new instance following immutable pattern)
        """
        # Check if existing reason is expired
        reason_is_expired = (
            self.expires_at is not None and
            self.expires_at < datetime.now()
        )

        # Check if content is the same (description and features)
        same_content = (
            self.description == mutation.description and
            self.features == mutation.features
        )

        if same_content and not reason_is_expired:
            # Update only expiration timestamp
            return LabelReason(
                pending=mutation.pending,
                description=self.description,
                features=self.features.copy(),
                created_at=self.created_at,  # Preserve original creation time
                expires_at=mutation.expires_at
            )
        else:
            # Replace entire reason (expired or different content)
            return LabelReason(
                pending=mutation.pending,
                description=mutation.description,
                features=mutation.features.copy(),
                created_at=datetime.now(),  # New creation time since content changed
                expires_at=mutation.expires_at
            )


@dataclass
class LabelStateInner:
    status: LabelStatus
    reasons: Dict[str, LabelReason]


@dataclass
class LabelState:
    """ Status and reasons for a label. Reason keys usually point to features/rules.

    """
    status: LabelStatus
    reasons: Dict[str, LabelReason]
    previous_states: List[LabelStateInner] = field(default_factory=list)

    def apply(self, mutation: 'EntityMutation') -> Optional['LabelState']:
        """
        Applies an EntityMutation to this LabelState with sophisticated conflict resolution.

        Implements label-state merging logic from label_provider_spec.md:
        - **Manual Protection**: Manual labels resist automatic updates unless expired
        - **Same Status Merging**: Merge reasons when status unchanged and not expired
        - **Status Changes**: Replace entire label state and preserve history

        Args:
            mutation: EntityMutation to apply to this label state

        Returns:
            Optional[LabelState]: New LabelState if changes were applied, None if no changes (e.g., manual protection)
        """
        prev_status = self.status
        next_status = mutation.status

        # Manual Label Protection: Manual labels resist automatic updates unless expired
        if (prev_status.is_manual() and
            next_status.is_automatic() and
            not self._reasons_are_expired()):
            return None  # No changes due to manual protection

        # Same Status Merging: Merge reasons when status unchanged and not expired
        if prev_status == next_status and not self._reasons_are_expired():
            # Create new state with updated reasons
            updated_reasons = self.reasons.copy()
            existing_reason = updated_reasons.get(mutation.reason_name)

            if existing_reason:
                # Update existing reason with expiration-aware logic
                updated_reason = existing_reason.update_with_mutation(mutation)
                updated_reasons[mutation.reason_name] = updated_reason
            else:
                # Add new reason
                new_reason = LabelReason(
                    pending=mutation.pending,
                    description=mutation.description,
                    features=mutation.features.copy(),
                    created_at=datetime.now(),
                    expires_at=mutation.expires_at
                )
                updated_reasons[mutation.reason_name] = new_reason

            return LabelState(
                status=self.status,
                reasons=updated_reasons,
                previous_states=self.previous_states.copy()
            )

        # Status Change: Replace entire label state and preserve history
        # Add current state to history before replacement
        history_entry = LabelStateInner(
            status=self.status,
            reasons=self.reasons.copy()
        )

        new_previous_states = self.previous_states.copy()
        new_previous_states.append(history_entry)

        # Keep history limited to 5 entries
        if len(new_previous_states) > 5:
            new_previous_states = new_previous_states[-5:]

        # Create new reason for the mutation
        new_reason = LabelReason(
            pending=mutation.pending,
            description=mutation.description,
            features=mutation.features.copy(),
            created_at=datetime.now(),
            expires_at=mutation.expires_at
        )

        return LabelState(
            status=mutation.status,
            reasons={mutation.reason_name: new_reason},
            previous_states=new_previous_states
        )


    def _reasons_are_expired(self) -> bool:
        """Checks if ALL reasons in this label state are expired."""
        if not self.reasons:
            return False

        now = datetime.now()
        for reason in self.reasons.values():
            if reason.expires_at is None or reason.expires_at > now:
                return False

        return True

    def compute_expiration(self) -> Optional[datetime]:
        """
        Computes label expiration from all its reasons.

        Label expires only when ALL its reasons are expired.
        """
        if not self.reasons:
            return None

        max_expiration = None

        for reason in self.reasons.values():
            if reason.expires_at is None:
                # If any reason never expires, label never expires
                return None
            elif max_expiration is None or reason.expires_at > max_expiration:
                max_expiration = reason.expires_at

        return max_expiration

@dataclass
class Labels:
    """ mapping of label names to their current state.
    """
    labels: Dict[str, LabelState] = field(default_factory=dict)
    expires_at: Optional[datetime] = None

    def compute_expiration(self) -> Optional[datetime]:
        """
        Computes entity-level expiration from all labels using conservative expiration logic.

        Entity expires when the latest-expiring label expires.
        If any label never expires (None), entity never expires (None).
        """
        if not self.labels:
            return None

        max_expiration = None

        for label_state in self.labels.values():
            label_expiration = label_state.compute_expiration()

            if label_expiration is None:
                # If any label never expires, entity never expires
                return None
            elif max_expiration is None or label_expiration > max_expiration:
                max_expiration = label_expiration

        return max_expiration


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
    status: LabelStatus = LabelStatus.ADDED
    pending: bool = False
    description: str = ''
    features: Dict[str, str] = field(default_factory=dict)
    expires_at: Optional[datetime] = None

    @staticmethod
    def merge(mutations: List['EntityMutation']) -> 'EntityMutation':
        """
        Merges multiple mutations for the same label using priority-based conflict resolution.

        Priority hierarchy (highest to lowest):
        - MANUALLY_ADDED (4)
        - MANUALLY_REMOVED (3)
        - ADDED (2)
        - REMOVED (1)

        Args:
            mutations: List of EntityMutation objects for the same label

        Returns:
            EntityMutation: Merged mutation with highest priority status
        """
        if not mutations:
            raise ValueError("Cannot merge empty list of mutations")

        if len(mutations) == 1:
            return mutations[0]

        # Priority mapping - higher numbers win
        priority_order = {
            LabelStatus.MANUALLY_ADDED: 4,
            LabelStatus.MANUALLY_REMOVED: 3,
            LabelStatus.ADDED: 2,
            LabelStatus.REMOVED: 1
        }

        # Find highest priority
        max_priority = max(priority_order[m.status] for m in mutations)

        # Filter to mutations with highest priority
        winning_mutations = [m for m in mutations if priority_order[m.status] == max_priority]

        if len(winning_mutations) == 1:
            return winning_mutations[0]

        # Multiple mutations with same priority - merge them
        primary = winning_mutations[0]

        # Combine descriptions (semicolon separated if different)
        descriptions = list(set(m.description for m in winning_mutations if m.description))
        merged_description = '; '.join(descriptions)

        # Combine features from all mutations
        merged_features = {}
        for mutation in winning_mutations:
            merged_features.update(mutation.features)

        # Use the latest expiration time
        expires_at_times = [m.expires_at for m in winning_mutations if m.expires_at is not None]
        merged_expires_at = max(expires_at_times) if expires_at_times else None

        # Use OR logic for pending (if any mutation is pending, result is pending)
        merged_pending = any(m.pending for m in winning_mutations)

        # Combine reason names (comma separated)
        reason_names = [m.reason_name for m in winning_mutations if m.reason_name]
        merged_reason_name = ','.join(reason_names)

        return EntityMutation(
            label_name=primary.label_name,
            reason_name=merged_reason_name,
            status=primary.status,  # All have same status due to filtering
            pending=merged_pending,
            description=merged_description,
            features=merged_features,
            expires_at=merged_expires_at
        )


@dataclass
class ApplyEntityMutationReply:
    added: List[str] = field(default_factory=list)
    removed: List[str] = field(default_factory=list)


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
