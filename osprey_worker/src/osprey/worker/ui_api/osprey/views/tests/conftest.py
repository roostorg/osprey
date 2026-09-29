"""Shared setup for the view tests.

These tests drive real endpoints against a real database, so most of them leave rows
behind. The test database is session-scoped, which means those rows outlive the test
that made them and are visible to every test that runs afterwards.
"""

from collections.abc import Iterator

import pytest
from osprey.worker.lib.storage.bulk_label_task import BulkLabelTask
from osprey.worker.lib.storage.postgres import scoped_session
from osprey.worker.lib.storage.queries import Query, SavedQuery
from osprey.worker.lib.storage.temporary_ability_token import TemporaryAbilityToken


@pytest.fixture(autouse=True)
def _clear_tables() -> Iterator[None]:
    """Start every view test *module* with the tables these tests write to empty."""
    with scoped_session(commit=True) as session:
        # `SavedQuery` is deleted before `Query` because it has a foreign key to it
        session.query(SavedQuery).delete()
        session.query(Query).delete()
        session.query(BulkLabelTask).delete()
        session.query(TemporaryAbilityToken).delete()
    yield
