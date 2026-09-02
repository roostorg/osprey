"""Storage-level tests for the rules table.

The endpoint behaviour lives in `ui_api/osprey/views/tests/test_rule_drafts.py`. These
cover the guarantees `Rule` itself makes, which a test going through the view cannot
distinguish from the view having caught something first.
"""

from collections.abc import Iterator

import pytest
from osprey.worker.lib.storage.postgres import scoped_session
from osprey.worker.lib.storage.rules import Rule, RuleNameTaken, RuleStatus
from sqlalchemy import UniqueConstraint


@pytest.fixture(autouse=True)
def _clear_rules() -> Iterator[None]:
    # The test database is session-scoped, so rows persist across tests. Start each
    # test with an empty table, or leaked rows trip the rule-name uniqueness check.
    with scoped_session(commit=True) as session:
        session.query(Rule).delete()
    yield


def _upsert(path: str, rule_name: str) -> Rule:
    return Rule.upsert(
        path=path,
        rule_name=rule_name,
        sml_source='x',
        summary='',
        author_email='local-dev@localhost',
    )


def test_rules_table_has_exactly_one_reachable_unique_constraint() -> None:
    """`Rule.upsert` maps any UniqueViolation to RuleNameTaken. That is only sound
    while `rule_name` is the sole unique constraint that can surface: `path` is
    absorbed by `ON CONFLICT (path) DO UPDATE` and `id` is a serial primary key.

    Adding another unique column would silently make `upsert` report unrelated
    collisions as rule-name conflicts. This fails first, and says why.
    """
    uniques = {tuple(c.columns.keys()) for c in Rule.__table__.constraints if isinstance(c, UniqueConstraint)}
    assert uniques == {('path',), ('rule_name',)}


def test_rule_name_uniqueness_is_enforced_by_the_database() -> None:
    """Two rows cannot share a rule name, and the failure is a domain error.

    The endpoint test for this goes through the view, which cannot distinguish "the
    constraint held" from "some other check caught it". This calls storage directly,
    so it fails if `unique=True` is ever dropped from the column.
    """
    _upsert('rules/one.sml', 'SharedName')
    with pytest.raises(RuleNameTaken):
        _upsert('rules/two.sml', 'SharedName')


def test_rule_name_taken_carries_the_holding_row() -> None:
    """The exception reports which row holds the name, so callers need not re-query."""
    _upsert('rules/one.sml', 'SharedName')
    with pytest.raises(RuleNameTaken) as excinfo:
        _upsert('rules/two.sml', 'SharedName')

    assert [rule.path for rule in excinfo.value.existing] == ['rules/one.sml']
    assert excinfo.value.rule_name == 'SharedName'


def test_upsert_keeping_its_own_rule_name_is_not_a_conflict() -> None:
    """Editing a draft in place re-writes its own rule_name to the same value.

    `ON CONFLICT (path) DO UPDATE` makes that an UPDATE, and setting a unique column to
    the value it already holds violates nothing -- which is why a rule_name conflict
    always implicates some *other* row, and why `get_all_with_rule_name` needs no
    "exclude this path" argument.
    """
    first = _upsert('rules/same.sml', 'StableName')
    again = Rule.upsert(
        path='rules/same.sml',
        rule_name='StableName',
        sml_source='y',
        summary='edited',
        author_email='local-dev@localhost',
    )
    assert again.id == first.id
    assert again.summary == 'edited'


def test_editing_a_deployed_rule_returns_it_to_draft() -> None:
    """Editing a deployed row moves it back to DRAFT.

    `upsert` sets `status` in its update set, so a row that was deployed and has since
    been edited stops claiming to match what is on disk. Documented on `upsert` and
    otherwise untested: the only other place a deployed status is asserted is the
    deploy endpoint test, which checks that deploying *sets* it, not that a later edit
    resets it.
    """
    rule = _upsert('rules/live.sml', 'LiveRule')
    Rule.mark_deployed(rule.id)

    deployed = Rule.get_one_with_id(rule.id)
    assert deployed is not None
    assert deployed.status == RuleStatus.DEPLOYED
    assert deployed.deployed_at is not None

    _upsert('rules/live.sml', 'LiveRule')  # same path -> ON CONFLICT (path) DO UPDATE

    edited = Rule.get_one_with_id(rule.id)
    assert edited is not None
    assert edited.status == RuleStatus.DRAFT
    # deployed_at is left alone: it records when this path was last deployed, which the
    # edit doesn't undo. Only `status` says whether the row still matches that deploy.
    assert edited.deployed_at is not None
