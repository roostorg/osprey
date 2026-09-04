"""Storage-level tests for the rules table.

The endpoint behaviour lives in `ui_api/osprey/views/tests/test_rule_drafts.py`. These
cover the guarantees `Rule` itself makes, which a test going through the view cannot
distinguish from the view having caught something first.
"""

import hashlib
from collections.abc import Iterator

import pytest
from osprey.worker.lib.storage.postgres import scoped_session
from osprey.worker.lib.storage.rules import Rule, RuleNameTaken, RuleStatus, content_id
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
    absorbed by `ON CONFLICT (lower(path)) DO UPDATE` -- a row colliding on `path`
    collides on `lower(path)` too -- and `id` is a serial primary key.

    Adding another unique column would silently make `upsert` report unrelated
    collisions as rule-name conflicts. This fails first, and says why.
    """
    uniques = {tuple(c.columns.keys()) for c in Rule.__table__.constraints if isinstance(c, UniqueConstraint)}
    assert uniques == {('path',), ('rule_name',)}


def test_rules_table_has_no_unabsorbed_unique_indexes() -> None:
    """The same argument as above, for unique *indexes*, which are a separate collection.

    `Index(..., unique=True)` is not a `UniqueConstraint` -- it lives in
    `__table__.indexes`, not `__table__.constraints` -- so the test above cannot see one
    and would keep passing while the premise it documents became false. A unique index
    that ON CONFLICT does not arbitrate on would surface as a `UniqueViolation` and be
    reported to the caller as `RuleNameTaken`.

    `ix_rules_lower_path_unique` is the one that exists, and `upsert` arbitrates on
    `lower(path)`, so it is absorbed. Anything else added here needs the same treatment
    or `upsert`'s error mapping has to stop assuming.
    """
    unique_indexes = {index.name for index in Rule.__table__.indexes if index.unique}
    assert unique_indexes == {'ix_rules_lower_path_unique'}


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

    `ON CONFLICT (lower(path)) DO UPDATE` makes that an UPDATE, and setting a unique column to
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

    _upsert('rules/live.sml', 'LiveRule')  # same path -> ON CONFLICT (lower(path)) DO UPDATE

    edited = Rule.get_one_with_id(rule.id)
    assert edited is not None
    assert edited.status == RuleStatus.DRAFT
    # deployed_at is left alone: it records when this path was last deployed, which the
    # edit doesn't undo. Only `status` says whether the row still matches that deploy.
    assert edited.deployed_at is not None


def test_cid_is_the_hash_of_the_stored_source() -> None:
    """`upsert` maintains `cid` itself, so it cannot disagree with `sml_source`.

    Callers never pass a cid. That is the point: a content address supplied alongside
    the content is one someone can get wrong, and a wrong one is worse than none --
    it would report a deployed file as modified, or as current when it isn't.
    """
    rule = _upsert('rules/hashed.sml', 'HashedRule')
    assert rule.cid == content_id(rule.sml_source)
    assert rule.cid == hashlib.sha256(rule.sml_source.encode('utf-8')).hexdigest()


def test_cid_follows_the_source_through_an_edit() -> None:
    """Editing a draft re-hashes it; the same text hashes back to the same cid.

    The first half is what makes the cid usable for drift detection at all. The second
    is what makes it usable as an identity: reverting an edit restores the original
    address rather than minting a new one.
    """
    original = _upsert('rules/edited.sml', 'EditedRule')
    original_cid = original.cid

    edited = Rule.upsert(
        path='rules/edited.sml',
        rule_name='EditedRule',
        sml_source='PostText == "different"',
        summary='',
        author_email='local-dev@localhost',
    )
    assert edited.cid != original_cid

    reverted = Rule.upsert(
        path='rules/edited.sml',
        rule_name='EditedRule',
        sml_source=original.sml_source,
        summary='',
        author_email='local-dev@localhost',
    )
    assert reverted.cid == original_cid


def test_request_deploy_moves_a_draft_out_of_draft() -> None:
    """The state an author sets when they cannot deploy it themselves."""
    rule = _upsert('rules/ready.sml', 'ReadyRule')
    assert rule.status == RuleStatus.DRAFT

    requested = Rule.request_deploy(rule.id)
    assert requested is not None
    assert requested.status == RuleStatus.DEPLOY_REQUESTED
    assert requested.deployed_at is None


def test_editing_a_requested_draft_returns_it_to_draft() -> None:
    """A request is for particular text, so changing the text withdraws it.

    Falls out of `upsert` setting `status` unconditionally -- the same line that returns
    a deployed draft to `DRAFT` on edit. Tested because it is the behaviour a reviewer
    depends on: a draft in the queue is the one they were asked to look at.
    """
    rule = _upsert('rules/withdrawn.sml', 'WithdrawnRule')
    Rule.request_deploy(rule.id)

    edited = Rule.upsert(
        path='rules/withdrawn.sml',
        rule_name='WithdrawnRule',
        sml_source='PostText == "changed"',
        summary='',
        author_email='local-dev@localhost',
    )
    assert edited.status == RuleStatus.DRAFT


def test_request_deploy_on_a_deployed_rule_reads_as_a_redeploy_request() -> None:
    """Requesting a deployed rule is allowed, and `deployed_at` keeps the distinction.

    Worth permitting rather than refusing: a rule whose file was edited or deleted on
    disk genuinely needs deploying again, and the row is the only place to say so.
    """
    rule = _upsert('rules/again.sml', 'AgainRule')
    Rule.mark_deployed(rule.id)

    requested = Rule.request_deploy(rule.id)
    assert requested is not None
    assert requested.status == RuleStatus.DEPLOY_REQUESTED
    # Still carries when it was last deployed, so this is distinguishable from a first
    # request on a never-deployed draft.
    assert requested.deployed_at is not None


def test_request_deploy_returns_none_for_an_unknown_draft() -> None:
    assert Rule.request_deploy(999999) is None


def test_paths_differing_only_in_case_are_one_draft() -> None:
    """`spam.sml` and `Spam.sml` are one file on a case-insensitive filesystem.

    Postgres compares `text` case-sensitively, so without the `lower(path)` index these
    would be two rows -- and deploying both would write one file, silently overwriting
    one draft with the other while the table still showed two. The upsert arbitrates on
    `lower(path)` so the second save edits the first row instead of creating a second.

    Storage-level rather than through the view, because `VALID_PATH` rejects the
    uppercase spelling before a request ever reaches here. This is the backstop for the
    ways into the table that do not go through the API.
    """
    first = _upsert('rules/spam.sml', 'SpamRule')
    second = _upsert('rules/Spam.sml', 'SpamRule')

    assert second.id == first.id
    with scoped_session() as session:
        assert session.query(Rule).count() == 1
        # `path` is not in the upsert's mutable set, so the row keeps the casing it was
        # first stored with rather than being renamed by a later save.
        assert session.query(Rule).one().path == 'rules/spam.sml'
