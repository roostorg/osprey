from __future__ import annotations

from datetime import datetime, timezone
from enum import StrEnum

from sqlalchemy import BigInteger, Column, DateTime, Enum, Text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from .postgres import Model, scoped_session


def _now() -> datetime:
    return datetime.now(timezone.utc)


class RuleDraftStatus(StrEnum):
    DRAFT = 'draft'
    DEPLOYED = 'deployed'


class RuleDraft(Model):
    """A staged SML rule draft.

    Drafts are authored and validated in the UI and live here so the people who
    operate Osprey can reference, edit, and deploy them without any external code
    host. Deploying writes the SML into the configured rules directory; the draft
    row stays as the record of what was deployed.

    One row per rule `path` (upserted on submit), so the table reads as the
    current set of drafts rather than an append-only history.
    """

    __tablename__ = 'rule_drafts'

    id: int = Column(BigInteger, primary_key=True, autoincrement=True)
    path: str = Column(Text, nullable=False, unique=True)
    rule_name: str = Column(Text, nullable=False)
    sml_source: str = Column(Text, nullable=False)
    summary: str = Column(Text, nullable=False, default='')
    # Osprey has no users table; identity is just an email with ACLs applied, so
    # this stores the author's email rather than a foreign key.
    author_email: str = Column(Text, nullable=False)
    status: RuleDraftStatus = Column(
        Enum(RuleDraftStatus, native_enum=False, length=32),
        nullable=False,
        default=RuleDraftStatus.DRAFT,
    )
    created_at: datetime = Column(DateTime(timezone=True), nullable=False, default=_now)
    updated_at: datetime = Column(DateTime(timezone=True), nullable=False, default=_now, onupdate=_now)
    deployed_at: datetime | None = Column(DateTime(timezone=True), nullable=True)  # type: ignore[misc]

    def to_json(self) -> dict[str, object]:
        return {
            'id': self.id,
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
    def upsert(cls, *, path: str, rule_name: str, sml_source: str, summary: str, author_email: str) -> 'RuleDraft':
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
            'status': RuleDraftStatus.DRAFT,
            'updated_at': now,
        }
        statement = (
            pg_insert(cls.__table__)
            .values(path=path, created_at=now, **mutable)
            .on_conflict_do_update(index_elements=[cls.path], set_=mutable)
        )
        with scoped_session(commit=True) as session:
            session.execute(statement)
            session.flush()
            draft = session.query(cls).filter(cls.path == path).one()
            session.expunge(draft)
            return draft

    @classmethod
    def list_all(cls) -> list['RuleDraft']:
        with scoped_session() as session:
            drafts = session.query(cls).order_by(cls.updated_at.desc()).all()
            session.expunge_all()
            return drafts

    @classmethod
    def get_one(cls, draft_id: int) -> 'RuleDraft | None':
        with scoped_session() as session:
            draft = session.query(cls).filter(cls.id == draft_id).first()
            if draft is not None:
                session.expunge(draft)
            return draft

    @classmethod
    def other_with_rule_name(cls, rule_name: str, *, exclude_path: str) -> 'RuleDraft | None':
        """A draft at a different path that already uses `rule_name`, if one exists.

        Rule names are global identifiers in SML, so two drafts sharing a name would
        collide once both deploy. Validation only sees deployed rules, not other
        drafts, so this catches the draft-vs-draft case that validation can't.
        """
        with scoped_session() as session:
            draft = session.query(cls).filter(cls.rule_name == rule_name, cls.path != exclude_path).first()
            if draft is not None:
                session.expunge(draft)
            return draft

    @classmethod
    def mark_deployed(cls, draft_id: int) -> 'RuleDraft | None':
        with scoped_session(commit=True) as session:
            draft = session.query(cls).filter(cls.id == draft_id).first()
            if draft is None:
                return None
            draft.status = RuleDraftStatus.DEPLOYED
            draft.deployed_at = _now()
            session.flush()
            session.expunge(draft)
            return draft
