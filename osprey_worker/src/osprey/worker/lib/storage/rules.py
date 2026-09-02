# pyright: reportAssignmentType=false
#
# `Mapped[T] = Column(...)` is the SQLAlchemy 1.4 idiom the mypy plugin understands:
# it gives `rule.id` the type `int` rather than `Column[BigInteger]` for every
# consumer. pyright has no such plugin and sees only the raw assignment, so it flags
# each declaration. Suppressed here -- ten identical errors in one file -- rather
# than leaving consumers to coerce at every call site, which is unbounded and grows.
from __future__ import annotations

from datetime import datetime, timezone
from enum import StrEnum

from psycopg2.errors import UniqueViolation
from sqlalchemy import BigInteger, Column, DateTime, Enum, Text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Mapped

from .postgres import Model, scoped_session


def _now() -> datetime:
    return datetime.now(timezone.utc)


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
    DRAFT = 'draft'
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

    id: Mapped[int] = Column(BigInteger, primary_key=True, autoincrement=True)
    path: Mapped[str] = Column(Text, nullable=False, unique=True)
    # Rule names are global identifiers in SML, so two rows sharing one would collide
    # once both deployed. Enforced here rather than only in the view: a pre-flight
    # check is check-then-act, and two concurrent creates can both find nothing.
    rule_name: Mapped[str] = Column(Text, nullable=False, unique=True)
    sml_source: Mapped[str] = Column(Text, nullable=False)
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
        """
        now = _now()
        mutable = {
            'rule_name': rule_name,
            'sml_source': sml_source,
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
                # constraint that can surface here. Guarded by
                # `test_rules_table_has_exactly_one_reachable_unique_constraint`.
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
