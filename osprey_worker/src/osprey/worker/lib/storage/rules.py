# pyright: reportAssignmentType=false
#
# `Mapped[T] = Column(...)` is the SQLAlchemy 1.4 idiom the mypy plugin understands:
# it gives `rule.id` the type `int` rather than `Column[BigInteger]` for every
# consumer. pyright has no such plugin and sees only the raw assignment, so it flags
# each declaration. Suppressed here -- ten identical errors in one file -- rather
# than leaving consumers to coerce at every call site, which is unbounded and grows.
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from enum import StrEnum

from psycopg2.errors import UniqueViolation
from sqlalchemy import BigInteger, CheckConstraint, Column, DateTime, Enum, Text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Mapped

from .postgres import Model, scoped_session


def _now() -> datetime:
    return datetime.now(timezone.utc)


def content_id(sml_source: str) -> str:
    """A content address for a rule's SML: the SHA-256 of its UTF-8 bytes, hex encoded.

    Hashes the source exactly as stored, with no normalisation. That means the id
    answers "is this the same text?" rather than "is this the same program", so
    reindenting a rule changes it -- which is the useful reading here, because the
    thing being compared is a file on disk that a human may have edited.

    Deploy writes `sml_source` verbatim, so `content_id(rule.sml_source)` equals the
    SHA-256 of the deployed file. That is what makes it possible to tell a rule that is
    deployed and current from one edited or deleted on disk since, without keeping a
    second copy of the text to diff against.
    """
    return hashlib.sha256(sml_source.encode('utf-8')).hexdigest()


class RuleNameTaken(Exception):
    """A write would have given two rows the same `rule_name`.

    Rule names are global identifiers in SML, so the column is unique. Raised in place
    of the raw `IntegrityError` so callers can answer this case specifically without
    reaching into driver internals -- and so any *other* integrity failure keeps
    propagating rather than being reported as a name collision.

    `existing` holds the rows already using the name, looked up where the conflict is
    detected rather than left for the caller to re-derive. Whole rows rather than just
    their paths, so callers choose what to surface.

    A list, even though the unique constraint means there is at most one. The lookup
    necessarily runs in a *second* transaction -- Postgres aborts the first one on the
    violation, so the read cannot share a snapshot with the write that failed -- and an
    empty list is the honest answer when the holder is gone by the time we look. It
    also lets callers map straight onto `defined_in_source_paths` without branching.
    """

    def __init__(self, rule_name: str, existing: 'list[Rule]') -> None:
        super().__init__(f'rule_name {rule_name!r} is already taken')
        self.rule_name = rule_name
        self.existing = existing


class RuleStatus(StrEnum):
    #: Being worked on. The state every save lands in, including a save that edits a
    #: draft out of either state below.
    DRAFT = 'draft'
    #: The author considers it finished and is asking someone to deploy it.
    #:
    #: Exists because authoring and deploying are separate abilities: a holder of
    #: `CAN_EDIT_RULES` alone can write a draft but not ship it, and without this there
    #: is nothing for them to say "this is ready" *with* -- the table would show `draft`
    #: for both work in progress and work waiting on a reviewer.
    #:
    #: Reachable from `DEPLOYED` as well as `DRAFT`, which reads as "a redeploy is
    #: wanted"; `deployed_at` stays set, so the two are distinguishable.
    DEPLOY_REQUESTED = 'deploy_requested'
    #: Written into the rules directory. Says what the API did, not what is currently on
    #: disk -- a file edited or removed since is only visible through the deploy plan's
    #: `rule_file.state`, which compares the stored `cid` against the file.
    DEPLOYED = 'deployed'


class Rule(Model):
    """A staged SML rule draft.

    Drafts are authored and validated in the UI and live here so the people who
    operate Osprey can reference, edit, and deploy them without any external code
    host. Deploying writes the SML into the configured rules directory; the draft
    row stays as the record of what was deployed.

    One row per rule `path` (upserted on submit), so the table reads as the
    current set of drafts rather than an append-only history.
    """

    __tablename__ = 'rules'

    # A path is a filename, and two filenames differing only in case are one file on a
    # case-insensitive filesystem -- the macOS default, so every developer's machine --
    # while being two rows here, because Postgres compares `text` case-sensitively.
    # Deploying both would write one file and silently discard one of the drafts.
    #
    # Requiring paths to be lowercase is what makes the two agree, and it is stated here
    # rather than only in `validators.rules.VALID_PATH` because the API is not the only
    # way to write a row and the invariant belongs where the data does. Uniqueness then
    # needs nothing special: if every path is lowercase, `path`'s existing unique
    # constraint is already case-insensitive uniqueness.
    #
    # A `CheckConstraint` rather than lowercasing on write, because silently storing
    # something other than what the caller asked for is how a row and a filename come to
    # disagree in the first place.
    __table_args__ = (CheckConstraint('path = lower(path)', name='ck_rules_path_is_lowercase'),)

    id: Mapped[int] = Column(BigInteger, primary_key=True, autoincrement=True)
    path: Mapped[str] = Column(Text, nullable=False, unique=True)
    # Rule names are global identifiers in SML, so two rows sharing one would collide
    # once both deployed. Enforced here rather than only in the view: a pre-flight
    # check is check-then-act, and two concurrent creates can both find nothing.
    rule_name: Mapped[str] = Column(Text, nullable=False, unique=True)
    sml_source: Mapped[str] = Column(Text, nullable=False)
    # Content address of `sml_source`, maintained by `upsert` rather than supplied by
    # callers so the two cannot disagree. Stored rather than computed on read so a
    # deployed file can be compared against the text that produced it -- see
    # `content_id`. Not unique: two paths may legitimately hold identical SML.
    cid: Mapped[str] = Column(Text, nullable=False)
    summary: Mapped[str] = Column(Text, nullable=False, default='')
    # Osprey has no users table; identity is just an email with ACLs applied, so
    # this stores the author's email rather than a foreign key.
    author_email: Mapped[str] = Column(Text, nullable=False)
    status: Mapped[RuleStatus] = Column(
        Enum(RuleStatus, native_enum=False, length=32),
        nullable=False,
        default=RuleStatus.DRAFT,
    )
    created_at: Mapped[datetime] = Column(DateTime(timezone=True), nullable=False, default=_now)
    updated_at: Mapped[datetime] = Column(DateTime(timezone=True), nullable=False, default=_now, onupdate=_now)
    # The SQLAlchemy mypy plugin infers `Mapped[datetime]` from the column's type and
    # doesn't account for `nullable=True`, so it rejects the honest `| None` annotation.
    # Suppressed rather than dropping the `None`: the column really is nullable until a
    # draft is deployed, and `serialize` and `RuleRecord` both branch on it.
    deployed_at: Mapped[datetime | None] = Column(DateTime(timezone=True), nullable=True)  # type: ignore[misc]

    def serialize(self) -> dict[str, object]:
        """A readable dict for logging and shell poking."""
        return {
            'id': str(self.id),
            'path': self.path,
            'rule_name': self.rule_name,
            'source': self.sml_source,
            'cid': self.cid,
            'summary': self.summary,
            'author': self.author_email,
            'status': str(self.status),
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'deployed_at': self.deployed_at.isoformat() if self.deployed_at else None,
        }

    @classmethod
    def upsert(cls, *, path: str, rule_name: str, sml_source: str, summary: str, author_email: str) -> 'Rule':
        """Create the draft for `path`, or update it in place if one already exists.

        Uses a single `INSERT ... ON CONFLICT DO UPDATE` so two concurrent saves of
        the same path can't both see "no row" and then race the unique constraint.
        Editing a deployed draft moves it back to `DRAFT` so the table reflects that
        the in-flight SML no longer matches what was last deployed.

        Paths are lowercase -- enforced by `ck_rules_path_is_lowercase` above -- so
        arbitrating on `path` is already case-insensitive and a non-lowercase path from a
        caller that bypassed the API fails the check rather than quietly becoming a second
        row for the same file.
        """
        now = _now()
        mutable = {
            'rule_name': rule_name,
            'sml_source': sml_source,
            'cid': content_id(sml_source),
            'summary': summary,
            'author_email': author_email,
            'status': RuleStatus.DRAFT,
            'updated_at': now,
        }
        statement = (
            pg_insert(cls.__table__)
            .values(path=path, created_at=now, **mutable)
            .on_conflict_do_update(index_elements=[cls.path], set_=mutable)
        )
        with scoped_session(commit=True) as session:
            try:
                session.execute(statement)
                session.flush()
            except IntegrityError as exc:
                # Roll back before propagating: `scoped_session` doesn't, and inside a
                # request it isn't the session's owner either, so the caller would
                # otherwise inherit an aborted transaction.
                session.rollback()
                # `path`'s unique constraint is absorbed by ON CONFLICT (path) above and
                # `id` is a serial primary key, so `rule_name` is the only unique
                # constraint that can surface here. `ck_rules_path_is_lowercase` is a
                # check, not a unique constraint, so it raises an IntegrityError that is
                # not a UniqueViolation and falls through to the re-raise below -- which
                # is right: a non-lowercase path is a caller error, not a name conflict.
                # Guarded by `test_rules_table_has_exactly_one_reachable_unique_constraint`
                # and `test_rules_table_has_no_unabsorbed_unique_indexes`.
                # Anything else -- a NOT NULL violation, say -- is not ours to explain.
                if isinstance(exc.orig, UniqueViolation):
                    raise RuleNameTaken(rule_name, existing=cls.get_all_with_rule_name(rule_name)) from exc
                raise
            draft = session.query(cls).filter(cls.path == path).one()
            session.expunge(draft)
            return draft

    @classmethod
    def list_all(cls) -> list['Rule']:
        with scoped_session() as session:
            drafts = session.query(cls).order_by(cls.updated_at.desc()).all()
            session.expunge_all()
            return drafts

    @classmethod
    def get_one_with_id(cls, draft_id: int) -> 'Rule | None':
        with scoped_session() as session:
            draft = session.query(cls).filter(cls.id == draft_id).first()
            if draft is not None:
                session.expunge(draft)
            return draft

    @classmethod
    def get_all_with_rule_name(cls, rule_name: str) -> list['Rule']:
        """Every draft using `rule_name` -- at most one, since the column is unique.

        Rule names are global identifiers in SML, so two drafts sharing one would
        collide once both deployed -- which is why the column is unique. Callers use
        this after an `IntegrityError` to name the offending file in the error, not to
        pre-check: a check before the write is check-then-act and races.

        No `exclude_path`: an upsert that keeps a row's own name violates nothing, so
        a `rule_name` conflict always means some *other* row holds it.
        """
        with scoped_session() as session:
            drafts = session.query(cls).filter(cls.rule_name == rule_name).all()
            session.expunge_all()
            return drafts

    @classmethod
    def request_deploy(cls, draft_id: int) -> 'Rule | None':
        """Mark a draft as awaiting deployment, or `None` if there is no such draft.

        Idempotent, and permitted from any state: requesting an already-requested draft
        changes nothing, and requesting a deployed one asks for a redeploy -- worth
        allowing, because a rule whose file was edited or deleted on disk genuinely
        needs deploying again, and `deployed_at` survives so the two are distinguishable.

        `updated_at` moves, because the column carries `onupdate` and this is an UPDATE.
        That is the wanted behaviour rather than an accident of the mapping: the list is
        ordered by it, so a requested draft rises to the top of the table a reviewer
        works from.
        """
        with scoped_session(commit=True) as session:
            draft = session.query(cls).filter(cls.id == draft_id).first()
            if draft is None:
                return None
            draft.status = RuleStatus.DEPLOY_REQUESTED
            session.flush()
            session.expunge(draft)
            return draft

    @classmethod
    def mark_deployed(cls, draft_id: int) -> 'Rule | None':
        with scoped_session(commit=True) as session:
            draft = session.query(cls).filter(cls.id == draft_id).first()
            if draft is None:
                return None
            draft.status = RuleStatus.DEPLOYED
            draft.deployed_at = _now()
            session.flush()
            session.expunge(draft)
            return draft
