# Labels Service API Specification

## Overview

The Labels Service manages entity labeling within Discord's Smite data services ecosystem. It provides functionality for applying, retrieving, and managing labels on various entity types (Users, Guilds, etc.) with support for both automatic and manual label statuses.

**Note**: This specification references the `LabelProvider` abstract base class and existing Python dataclasses from the codebase (`Labels`, `EntityMutation`, `ApplyEntityMutationReply`, `LabelReason`, `LabelState`, etc.).

## Core Concepts

### Entity
An entity represents a labeled object in the system, identified by:
- **Type**: Entity type (e.g., "User", "Guild")
- **ID**: Unique identifier within the type namespace

### Label Status Hierarchy
Labels have a status hierarchy that determines precedence during mutations:
1. **ManuallyAdded** (Priority 4) - Manually applied positive label
2. **ManuallyRemoved** (Priority 3) - Manually applied negative label
3. **Added** (Priority 2) - Automatically applied positive label
4. **Removed** (Priority 1) - Automatically applied negative label

### Mutation Operations
- **Added**: New reason added for a label
- **Updated**: Existing reason modified
- **Unchanged**: Reason exists but unchanged
- **Removed**: Reason removed from label

### Label Expiration System

The service implements a **three-level hierarchical expiration system** that determines when labels and entities can be safely removed from storage:

#### **Level 1: Reason Expiration**
Each `LabelReason` has an optional `expires_at` timestamp:
- **`None`** = Reason never expires (permanent)
- **`Some(timestamp)`** = Reason expires at specified datetime

#### **Level 2: Label Expiration**
A label is considered expired only when **ALL** its reasons are expired:
- If any reason is permanent (`None`), the label never expires
- If any reason has a future expiration, the label remains active
- Only when all reasons are past their expiration does the label expire

#### **Level 3: Entity Expiration**
An entity's expiration is the **latest** expiration among all its labels:
- If any label never expires, the entity never expires
- Otherwise, entity expires when the last label expires
- Automatically recomputed after any label mutation

**Key Principle**: The system uses **conservative expiration** - entities and labels persist until ALL constituent components are definitively expired.

---

## API Contracts

### 1. Get Entity

**Method**: `get_from_service`

**Description**: Retrieves a single entity's current label state

**Function Signature**:
```python
def get_from_service(self, key: EntityT[Any]) -> Labels:
    """
    Given a key, return the labels for that entity.
    """
```

**Parameters**:
- `key: EntityT[Any]` - Entity identifier with type and id

**Returns**:
- `Labels` - The labels for the entity (existing dataclass from codebase)

**Labels Structure** (already exists in codebase):
```python
@dataclass
class Labels:
    labels: Dict[str, LabelState] = field(default_factory=dict)  # label_name -> LabelState
    expires_at: Optional[datetime] = None  # Optional expiration
```

**Behavior**:
- No entity key validation (allows retrieval of "bad" entities for investigation)
- Returns empty Labels if not found
- Executes read-only operation on the underlying storage

**Expiration Handling**:
- **Returns expired data**: Retrieves labels regardless of expiration status
- **No automatic cleanup**: Expired labels remain in storage until explicitly removed
- **Expiration metadata included**: Labels include computed `expires_at` timestamp for the entire entity
- **Real-time expiration calculation**: Entity-level `expires_at` is computed from all active label expirations

**Expiration Logic in Response**:
```python
# Entity expires_at = latest expiration among all labels
# If any label never expires (None), entity never expires (None)
labels = Labels(
    labels={"spam": LabelState(...), "verified": LabelState(...)},
    expires_at=max_expiration_among_labels_or_none()
)
```

**Error Handling**:
- Raises exceptions for internal failures
- Invalid entity key format raises validation exception

---

### 2. Get Entity Batch

**Method**: `batch_get_from_service`

**Description**: Retrieves multiple entities in a single request for efficient batch operations

**Function Signature**:
```python
def batch_get_from_service(self, keys: Sequence[EntityT[Any]]) -> Sequence[Result[Labels, Exception]]:
    """
    Batch get labels for multiple entities.
    """
```

**Parameters**:
- `keys: Sequence[EntityT[Any]]` - List of entity keys to retrieve

**Returns**:
- `Sequence[Result[Labels, Exception]]` - Results for each requested entity, either Ok(Labels) or Err(Exception)

**Logic Behind `batch_get_from_service`**:

The batch operation is implemented as concurrent execution of individual `get_from_service` calls:

1. **Concurrent Processing**: Each entity key is processed in parallel using async tasks
2. **Error Isolation**: If one entity fails, others continue processing
3. **Graceful Degradation**: Failed retrievals return Err(Exception) rather than failing the entire batch
4. **Result Wrapping**: Each result is wrapped in a Result type for explicit error handling

**Implementation Pattern**:
```python
# Typical implementation pattern
def batch_get_from_service(self, keys: Sequence[EntityT[Any]]) -> Sequence[Result[Labels, Exception]]:
    results = []
    for key in keys:
        try:
            labels = self.get_from_service(key)
            results.append(Ok(labels))
        except Exception as e:
            results.append(Err(e))
    return results
```

**Performance Characteristics**:
- O(1) time complexity relative to batch size due to concurrency
- Memory usage scales linearly with batch size
- Network efficiency through single round-trip

**Expiration Handling**:
- **Inherited behavior**: Each entity in the batch follows the same expiration rules as `get_from_service`
- **No batch-level expiration logic**: Each entity's expiration is computed independently
- **Concurrent expiration calculation**: Entity expirations computed in parallel across all requested entities
- **Consistent expiration semantics**: All entities return their current expiration state regardless of whether they're expired

---

### 3. Apply Entity Mutations

**Method**: `apply_entity_mutation`

**Description**: Applies label mutations to an entity with conflict resolution and priority handling

**Function Signature**:
```python
def apply_entity_mutation(
    self, entity_key: EntityT[Any], mutations: List[EntityMutation]
) -> ApplyEntityMutationReply:
    """
    Apply mutations to an entity's labels.
    """
```

**Parameters**:
- `entity_key: EntityT[Any]` - Entity identifier with type and id
- `mutations: List[EntityMutation]` - List of mutations to apply

**Returns**:
- `ApplyEntityMutationReply` - Result of the mutation operation (existing dataclass from codebase)

**Related Data Structures** (already exist in codebase):
```python
# EntityMutation dataclass (already exists in codebase)
@dataclass
class EntityMutation:
    label_name: str = ''
    reason_name: str = ''
    status: LabelStatus = LabelStatus.ADDED
    pending: bool = False
    description: str = ''
    features: Dict[str, str] = field(default_factory=dict)  # Note: currently 'features', spec suggests 'metadata'
    expires_at: Optional[datetime] = None

# LabelReason dataclass (already exists in codebase)
@dataclass
class LabelReason:
    pending: bool = False
    description: str = ''
    features: Dict[str, str] = field(default_factory=dict)  # Note: currently 'features', spec suggests 'metadata'
    created_at: datetime | None = None
    expires_at: datetime | None = None

# ApplyEntityMutationReply dataclass (already exists in codebase)
@dataclass
class ApplyEntityMutationReply:
    added: List[str] = field(default_factory=list)
    removed: List[str] = field(default_factory=list)
    unchanged: List[str] = field(default_factory=list)
    dropped: List[EntityMutation] = field(default_factory=list)
```

**Logic Behind `apply_entity_mutation`**:

This is the core business logic method that handles the complex mutation process:

#### Phase 1: Mutation Merging and Conflict Resolution
```python
# High-level flow
label_states_to_apply, dropped = merge_mutations(mutations)
```

**Priority-Based Conflict Resolution**:
- Groups mutations by label name
- Determines dominant status using priority hierarchy
- Drops lower-priority conflicting mutations
- Preserves all reasons that match the dominant status

**Example Conflict Resolution**:
```
Input: [
  {label: "spam", status: ADDED, reason: "auto_detection"},
  {label: "spam", status: MANUALLY_REMOVED, reason: "human_review"}
]

Result:
- Dominant: MANUALLY_REMOVED (priority 3 > 2)
- Applied: {label: "spam", status: MANUALLY_REMOVED, reasons: ["human_review"]}
- Dropped: [{label: "spam", status: ADDED, reason: "auto_detection"}]
```

#### Phase 2: Database Transaction Execution
```python
# Conceptual implementation
async def execute_mutations(entity_key: EntityT[Any], label_states_to_apply: Dict[str, LabelState]):
    merge_results = {}
    for label_name, label_state in label_states_to_apply.items():
        merge_result = entity.merge_with(label_name, label_state)
        merge_results[label_name] = merge_result

    # Commit immediately
    return merge_results
```

**Entity State Merging**:
- For each label, merges new state with existing state
- Tracks what operations occurred (Added, Updated, Unchanged, Removed)
- Maintains history of previous states (up to 5 entries)
- Updates expiration timestamps
- **Expiration-aware merging**: Respects expiration rules during state transitions

#### Phase 3: Disagreement Tracking
```python
self.insert_label_disagreements(entity_key, merge_results)
```

**Disagreement Detection Logic**:
- Detects when automatic labels transition to manual status
- Records the automatic reasons that were "disagreed with"
- Only triggers when:
  1. Status changes from automatic → manual
  2. The added/removed state flips (added→removed or removed→added)
  3. Previous status was not already manual

**Disagreement Example**:
```
Previous: {status: ADDED, reasons: ["auto_spam_detection", "pattern_match"]}
New:      {status: MANUALLY_REMOVED, reasons: ["human_review"]}

Disagreement Recorded: {
  label: "spam",
  disagreed_reasons: ["auto_spam_detection", "pattern_match"],
  manual_status: MANUALLY_REMOVED
}
```

**Error Handling**:
- Entity key validation with specific format requirements
- Transaction rollback on merge failures
- Graceful handling of individual reason merge failures

**Performance Optimizations**:
- Single database transaction for all label mutations
- Immediate commit strategy for consistency
- Batch processing of disagreement insertions

#### Expiration Handling in Mutations

The mutation system implements sophisticated expiration-aware logic at multiple levels:

##### **Manual Label Protection**
```python
# Manual labels resist automatic updates unless expired
if (prev_status.is_manual() and
    next_status.is_automatic() and
    not self.current_state.reasons_are_expired()):
    # Ignore automatic update - manual label still valid
    return LabelStateMergeResult.unchanged()
```

**Behavior**:
- Manual labels (MANUALLY_ADDED/MANUALLY_REMOVED) block automatic updates
- Protection only applies when manual label's reasons are NOT expired
- Once manual reasons expire, automatic updates can override

##### **Reason-Level Expiration Logic**
```python
# Individual reason update logic
reason_is_expired = (
    existing_reason.expires_at is not None and
    existing_reason.expires_at < datetime.now()
)

if same_content and not reason_is_expired:
    # Update only expiration timestamp
    existing_reason.expires_at = new_expiry
else:
    # Replace entire reason (expired or different content)
    replace_reason(new_reason)
```

**Behaviors**:
- **Expired reason replacement**: Expired reasons get completely replaced, not updated
- **Expiration-only updates**: Non-expired reasons with same content only update `expires_at`
- **Content change handling**: Any content change creates new reason regardless of expiration

##### **Entity-Level Expiration Recomputation**
After any successful mutation:
```python
if mutation_result.did_change():
    entity.expires_at = compute_expires_at()  # Recompute from all labels
```

**Computation Logic**:
1. For each label, compute expiration from all its reasons
2. Take maximum expiration across all labels
3. If any label never expires (None), entity never expires (None)

##### **Expiration Impact on Status Transitions**

**Same Status Merging**:
```python
if (prev_status == next_status and
    not self.reasons_are_expired()):
    # Merge reasons when status unchanged and not expired
    pass
```

**Status Change Scenarios**:
```python
if prev_status != next_status or self.reasons_are_expired():
    # Status changed OR previous reasons expired
    # - Remove all old reasons
    # - Add all new reasons
    # - Update label status
    # - Preserve history
    pass
```

**Practical Examples**:

**Example 1: Manual Protection**
```
Existing: {status: MANUALLY_REMOVED, reasons: [{expires_at: 2024-12-31}]}
Incoming: {status: ADDED, reasons: [{expires_at: 2024-06-01}]}
Current time: 2024-06-15

Result: IGNORED (manual label not expired, automatic update blocked)
```

**Example 2: Expired Manual Override**
```
Existing: {status: MANUALLY_ADDED, reasons: [{expires_at: 2024-01-01}]}
Incoming: {status: REMOVED, reasons: [{expires_at: 2024-12-31}]}
Current time: 2024-06-15

Result: APPLIED (manual label expired, automatic update allowed)
Status: MANUALLY_ADDED → REMOVED
```

**Example 3: Reason Expiration Update**
```
Existing reason: {content: "spam detected", expires_at: 2024-06-01}
Incoming reason: {content: "spam detected", expires_at: 2024-12-31}
Current time: 2024-06-15

Result: UPDATE (same content, extend expiration only)
Operation: MutationOperation::Updated
```

---

### 4. LabelProvider Interface Details

**Cache TTL**:
```python
def cache_ttl(self) -> Optional[timedelta]:
    return timedelta(minutes=5)
```
The `LabelProvider` base class includes a `cache_ttl()` method that returns `timedelta(minutes=5)` by default, allowing implementations to specify how long results should be cached.

**External Service Integration**:
The `LabelProvider` extends `ExternalService[EntityT[Any], Labels]`, providing built-in caching and external service patterns for implementations.

**Abstract Methods**:
Implementations must provide the three core methods:
- `get_from_service(key: EntityT[Any]) -> Labels`
- `batch_get_from_service(keys: Sequence[EntityT[Any]]) -> Sequence[Result[Labels, Exception]]`
- `apply_entity_mutation(entity_key: EntityT[Any], mutations: List[EntityMutation]) -> ApplyEntityMutationReply`

---

## Data Types

### Label Status
```python
# LabelStatus enum (already exists in codebase)
class LabelStatus(IntEnum):
    ADDED = 0              # Automatically applied positive label
    REMOVED = 1            # Automatically applied negative label
    MANUALLY_ADDED = 2     # Manually applied positive label
    MANUALLY_REMOVED = 3   # Manually applied negative label
```

### Mutation Operation
```python
class MutationOperation(Enum):
    ADDED = 0      # New label/reason added
    UPDATED = 1    # Existing label/reason modified
    UNCHANGED = 2  # Label/reason exists but unchanged
    REMOVED = 3    # Label/reason removed
```

### Core Data Structures
```python
# LabelReason dataclass (already exists in codebase)
@dataclass
class LabelReason:
    pending: bool = False
    description: str = ''
    features: Dict[str, str] = field(default_factory=dict)  # Note: spec suggests 'metadata'
    created_at: datetime | None = None
    expires_at: datetime | None = None  # Optional: when this reason expires

# LabelState dataclass (already exists in codebase)
@dataclass
class LabelState:
    status: LabelStatus
    reasons: Dict[str, LabelReason]
    previous_states: List[LabelStateInner] = field(default_factory=list)

@dataclass
class LabelStateInner:
    status: LabelStatus
    reasons: Dict[str, LabelReason]
```

**Expiration Semantics**:
- **`expires_at = None`**: Reason never expires (permanent)
- **`expires_at = datetime`**: Reason expires at specified time
- **Expired reasons**: Past `expires_at` timestamp, eligible for replacement
- **Description templating**: Supports variable substitution from features/metadata

---

## Architecture Considerations

### Consistency Model
- **Strong Consistency**: Entity mutations are applied atomically within single transactions
- **Eventually Consistent**: Disagreement tracking is fire-and-forget for performance

### Scalability
- **Horizontal Scaling**: Stateless service design supports multiple instances
- **Token Range Scanning**: Enables distributed processing across the keyspace
- **Batch Operations**: Reduces network overhead for bulk operations

### Reliability
- **Error Isolation**: Batch operations don't fail entirely due to individual entity errors
- **Graceful Degradation**: Failed operations return default responses rather than errors
- **Comprehensive Metrics**: Tracks operation latency and error rates

### Expiration Management
- **Conservative Expiration**: Entities persist until ALL constituent reasons are definitively expired
- **No Automatic Cleanup**: Service does not automatically remove expired data from storage
- **Expiration Metadata**: All operations provide current expiration state for downstream cleanup processes
- **Manual Override Protection**: Expired automatic labels allow manual overrides to take precedence
- **Real-time Expiration Calculation**: Entity expiration computed fresh on every access or mutation

### Security
- **Entity Key Validation**: Prevents invalid entity access
- **Signed Token Queries**: Secure token range scanning
- **No Entity Key Validation for Reads**: Allows investigation of malformed entities

This API specification provides the foundation for integrating with the Labels Service while understanding the complex business logic that handles label precedence, conflict resolution, disagreement tracking, and sophisticated expiration management across the three-level hierarchy of reasons, labels, and entities.