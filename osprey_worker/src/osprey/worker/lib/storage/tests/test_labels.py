from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from osprey.engine.language_types.entities import EntityT
from osprey.worker.lib.osprey_shared.labels import (
    EntityLabelMutation,
    EntityLabels,
    LabelReason,
    LabelReasons,
    LabelState,
    LabelStatus,
    MutationDropReason,
)
from osprey.worker.lib.storage.labels import LabelsProvider, LabelsServiceBase


class MockLabelsService(LabelsServiceBase):
    """Mock implementation of LabelsServiceBase for testing"""

    def __init__(self):
        self.storage: dict[tuple[str, str], EntityLabels] = {}

    def read_labels(self, entity: EntityT[Any]) -> EntityLabels:
        key = (entity.type, entity.id)
        return self.storage.get(key, EntityLabels())

    def read_modify_write_labels_atomically(self, entity: EntityT[Any]):
        """Context manager that yields labels for atomic read-modify-write operations"""
        from contextlib import contextmanager

        @contextmanager
        def _context():
            # Read current labels
            labels = self.read_labels(entity)
            # Yield for modification
            yield labels
            # Write modified labels back
            key = (entity.type, entity.id)
            self.storage[key] = labels

        return _context()


@pytest.fixture
def labels_provider() -> LabelsProvider:
    """Fixture that provides a LabelsProvider with a mock service"""
    return LabelsProvider(MockLabelsService())


@pytest.fixture
def now() -> datetime:
    """Fixture that provides a consistent 'now' timestamp"""
    return datetime.now()


def test_compute_new_labels_from_mutations_adds_new_label(labels_provider: LabelsProvider, now: datetime):
    """Test adding a new label to an empty EntityLabels"""
    old_labels = EntityLabels()
    mutations = [
        EntityLabelMutation(
            label_name='test_label',
            reason_name='test_reason',
            status=LabelStatus.ADDED,
            pending=False,
            description='Test description',
            features={},
            expires_at=now + timedelta(days=1),
        )
    ]

    result = labels_provider._compute_new_labels_from_mutations(old_labels, mutations)

    assert 'test_label' in result.new_entity_labels.labels
    assert 'test_label' in result.labels_added
    assert len(result.labels_removed) == 0
    assert len(result.labels_updated) == 0
    assert len(result.dropped_mutations) == 0


def test_compute_new_labels_from_mutations_cannot_remove_unexpired_manual_label(
    labels_provider: LabelsProvider, now: datetime
):
    """Test removing an unexpired manually-added label"""
    old_labels = EntityLabels(
        labels={
            'test_label': LabelState(
                status=LabelStatus.MANUALLY_ADDED,
                reasons=LabelReasons(
                    {
                        'reason1': LabelReason(
                            description='original',
                            created_at=now,
                            expires_at=datetime.now(timezone.utc) + timedelta(days=5),
                        )
                    }
                ),
            )
        }
    )
    removal_mut = EntityLabelMutation(
        label_name='test_label',
        reason_name='removal_reason',
        status=LabelStatus.REMOVED,
        pending=False,
        description='Removing label',
        features={},
        expires_at=None,
    )

    mutations = [removal_mut]

    result = labels_provider._compute_new_labels_from_mutations(old_labels, mutations)

    assert result.new_entity_labels.labels['test_label'].status == LabelStatus.MANUALLY_ADDED
    assert len(result.labels_removed) == 0
    assert len(result.labels_added) == 0
    assert len(result.labels_updated) == 0
    assert len(result.dropped_mutations) == 1
    assert result.dropped_mutations[0].mutation == removal_mut


def test_compute_new_labels_from_mutations_updates_existing_label_same_status(
    labels_provider: LabelsProvider, now: datetime
):
    """Test updating an existing label with the same status (adds a new reason)"""
    old_labels = EntityLabels(
        labels={
            'test_label': LabelState(
                status=LabelStatus.ADDED,
                reasons=LabelReasons({'reason1': LabelReason(description='original', created_at=now)}),
            )
        }
    )
    mutations = [
        EntityLabelMutation(
            label_name='test_label',
            reason_name='reason2',
            status=LabelStatus.ADDED,
            pending=False,
            description='Additional reason',
            features={},
            expires_at=None,
        )
    ]

    result = labels_provider._compute_new_labels_from_mutations(old_labels, mutations)

    assert 'test_label' in result.labels_updated
    assert len(result.labels_added) == 0
    assert len(result.labels_removed) == 0
    assert 'reason1' in result.new_entity_labels.labels['test_label'].reasons
    assert 'reason2' in result.new_entity_labels.labels['test_label'].reasons


def test_compute_new_labels_from_mutations_drops_conflicting_mutations(labels_provider: LabelsProvider, now: datetime):
    """Test that conflicting mutations (same label, different status) result in drops"""
    old_labels = EntityLabels()
    mutations = [
        EntityLabelMutation(
            label_name='test_label',
            reason_name='add_reason',
            status=LabelStatus.ADDED,
            pending=False,
            description='Add label',
            features={},
            expires_at=None,
        ),
        EntityLabelMutation(
            label_name='test_label',
            reason_name='remove_reason',
            status=LabelStatus.REMOVED,
            pending=False,
            description='Remove label',
            features={},
            expires_at=None,
        ),
    ]

    result = labels_provider._compute_new_labels_from_mutations(old_labels, mutations)

    # One mutation should be dropped due to conflict
    assert len(result.dropped_mutations) == 1
    assert result.dropped_mutations[0].reason == MutationDropReason.CONFLICTING_MUTATION
    # The higher priority status (ADDED > REMOVED) should win
    assert result.new_entity_labels.labels['test_label'].status == LabelStatus.ADDED


def test_compute_new_labels_from_mutations_manual_blocks_automatic(labels_provider: LabelsProvider, now: datetime):
    """Test that manual status blocks automatic status changes"""
    old_labels = EntityLabels(
        labels={
            'test_label': LabelState(
                status=LabelStatus.MANUALLY_REMOVED,
                reasons=LabelReasons({'manual_reason': LabelReason(description='manually removed', created_at=now)}),
            )
        }
    )
    mutations = [
        EntityLabelMutation(
            label_name='test_label',
            reason_name='auto_reason',
            status=LabelStatus.ADDED,
            pending=False,
            description='Try to add automatically',
            features={},
            expires_at=None,
        )
    ]

    result = labels_provider._compute_new_labels_from_mutations(old_labels, mutations)

    # The automatic mutation should be dropped
    assert len(result.dropped_mutations) == 1
    assert result.dropped_mutations[0].reason == MutationDropReason.CANNOT_OVERRIDE_MANUAL
    # The label should still be manually removed
    assert result.new_entity_labels.labels['test_label'].status == LabelStatus.MANUALLY_REMOVED


def test_compute_new_labels_from_mutations_manual_overrides_automatic(labels_provider: LabelsProvider, now: datetime):
    """Test that manual status can override automatic status"""
    old_labels = EntityLabels(
        labels={
            'test_label': LabelState(
                status=LabelStatus.ADDED,
                reasons=LabelReasons({'auto_reason': LabelReason(description='automatic add', created_at=now)}),
            )
        }
    )
    mutations = [
        EntityLabelMutation(
            label_name='test_label',
            reason_name='manual_reason',
            status=LabelStatus.MANUALLY_REMOVED,
            pending=False,
            description='Manual removal',
            features={},
            expires_at=None,
        )
    ]

    result = labels_provider._compute_new_labels_from_mutations(old_labels, mutations)

    # Manual should override automatic
    assert result.new_entity_labels.labels['test_label'].status == LabelStatus.MANUALLY_REMOVED
    assert 'test_label' in result.labels_removed
    assert len(result.dropped_mutations) == 0


def test_compute_new_labels_from_mutations_multiple_labels(labels_provider: LabelsProvider, now: datetime):
    """Test mutations affecting multiple different labels"""
    old_labels = EntityLabels(
        labels={
            'existing_label': LabelState(
                status=LabelStatus.ADDED,
                reasons=LabelReasons({'reason1': LabelReason(description='exists', created_at=now)}),
            )
        }
    )
    mutations = [
        EntityLabelMutation(
            label_name='new_label',
            reason_name='reason_new',
            status=LabelStatus.ADDED,
            pending=False,
            description='New label',
            features={},
            expires_at=None,
        ),
        EntityLabelMutation(
            label_name='existing_label',
            reason_name='reason_update',
            status=LabelStatus.ADDED,
            pending=False,
            description='Update existing',
            features={},
            expires_at=None,
        ),
    ]

    result = labels_provider._compute_new_labels_from_mutations(old_labels, mutations)

    assert 'new_label' in result.labels_added
    assert 'existing_label' in result.labels_updated
    assert len(result.new_entity_labels.labels) == 2


def test_compute_new_labels_from_mutations_expired_label_can_be_changed(labels_provider: LabelsProvider, now: datetime):
    """Test that an expired label state can be changed"""
    # Create an expired label (expires_at in the past)
    old_labels = EntityLabels(
        labels={
            'test_label': LabelState(
                status=LabelStatus.MANUALLY_REMOVED,
                reasons=LabelReasons(
                    {
                        'expired_reason': LabelReason(
                            description='expired',
                            created_at=now - timedelta(days=2),
                            expires_at=now - timedelta(days=1),
                        )
                    }
                ),
            )
        }
    )
    mutations = [
        EntityLabelMutation(
            label_name='test_label',
            reason_name='new_reason',
            status=LabelStatus.ADDED,
            pending=False,
            description='Add after expiration',
            features={},
            expires_at=None,
        )
    ]

    result = labels_provider._compute_new_labels_from_mutations(old_labels, mutations)

    # Should be able to change expired manual status
    assert result.new_entity_labels.labels['test_label'].status == LabelStatus.ADDED
    assert 'test_label' in result.labels_added
    assert len(result.dropped_mutations) == 0


def test_compute_new_labels_from_mutations_merge_multiple_mutations_same_label(
    labels_provider: LabelsProvider, now: datetime
):
    """Test that multiple mutations for the same label with same status are merged"""
    old_labels = EntityLabels()
    mutations = [
        EntityLabelMutation(
            label_name='test_label',
            reason_name='reason1',
            status=LabelStatus.ADDED,
            pending=False,
            description='First reason',
            features={'key1': 'value1'},
            expires_at=None,
        ),
        EntityLabelMutation(
            label_name='test_label',
            reason_name='reason2',
            status=LabelStatus.ADDED,
            pending=False,
            description='Second reason',
            features={'key2': 'value2'},
            expires_at=None,
        ),
    ]

    result = labels_provider._compute_new_labels_from_mutations(old_labels, mutations)

    # Both reasons should be present
    assert 'reason1' in result.new_entity_labels.labels['test_label'].reasons
    assert 'reason2' in result.new_entity_labels.labels['test_label'].reasons
    assert len(result.dropped_mutations) == 0
    assert 'test_label' in result.labels_added


def test_compute_new_labels_from_mutations_preserves_old_labels(labels_provider: LabelsProvider, now: datetime):
    """Test that old_labels snapshot is captured in result before modifications"""
    old_labels = EntityLabels(
        labels={
            'existing_label': LabelState(
                status=LabelStatus.ADDED,
                reasons=LabelReasons({'reason1': LabelReason(description='exists', created_at=now)}),
            )
        }
    )
    mutations = [
        EntityLabelMutation(
            label_name='new_label',
            reason_name='reason_new',
            status=LabelStatus.ADDED,
            pending=False,
            description='New label',
            features={},
            expires_at=None,
        )
    ]

    result = labels_provider._compute_new_labels_from_mutations(old_labels, mutations)

    # result.old_entity_labels should be a snapshot before modifications
    assert 'new_label' not in result.old_entity_labels.labels
    assert 'existing_label' in result.old_entity_labels.labels
    # The input labels parameter IS modified in place
    assert 'new_label' in old_labels.labels
    # result.new_entity_labels should reference the same modified object
    assert result.new_entity_labels is old_labels


def test_compute_new_labels_from_mutations_empty_mutations(labels_provider: LabelsProvider, now: datetime):
    """Test behavior with no mutations"""
    old_labels = EntityLabels(
        labels={
            'existing_label': LabelState(
                status=LabelStatus.ADDED,
                reasons=LabelReasons({'reason1': LabelReason(description='exists', created_at=now)}),
            )
        }
    )
    mutations = []

    result = labels_provider._compute_new_labels_from_mutations(old_labels, mutations)

    # Nothing should change
    assert len(result.labels_added) == 0
    assert len(result.labels_removed) == 0
    assert len(result.labels_updated) == 0
    assert len(result.dropped_mutations) == 0
    assert result.new_entity_labels.labels == old_labels.labels


def test_compute_new_labels_from_mutations_with_pending_status(labels_provider: LabelsProvider, now: datetime):
    """Test mutations with pending=True"""
    old_labels = EntityLabels()
    mutations = [
        EntityLabelMutation(
            label_name='pending_label',
            reason_name='pending_reason',
            status=LabelStatus.ADDED,
            pending=True,
            description='Pending label',
            features={},
            expires_at=None,
        )
    ]

    result = labels_provider._compute_new_labels_from_mutations(old_labels, mutations)

    assert 'pending_label' in result.new_entity_labels.labels
    assert 'pending_label' in result.labels_added
    # Verify the reason is marked as pending
    reasons = result.new_entity_labels.labels['pending_label'].reasons
    assert reasons['pending_reason'].pending is True
