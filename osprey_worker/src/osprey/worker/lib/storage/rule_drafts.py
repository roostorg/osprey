from __future__ import annotations

from datetime import datetime, timezone
from enum import StrEnum

from sqlalchemy import BigInteger, Column, DateTime, Enum, Text

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

        Editing a deployed draft moves it back to `DRAFT` so the table reflects
        that the in-flight SML no longer matches what was last deployed.
        """
        with scoped_session(commit=True) as session:
            draft = session.query(cls).filter(cls.path == path).first()
            if draft is None:
                draft = cls(path=path)
                session.add(draft)
            draft.rule_name = rule_name
            draft.sml_source = sml_source
            draft.summary = summary
            draft.author_email = author_email
            draft.status = RuleDraftStatus.DRAFT
            session.flush()
            session.expunge(draft)
            return draft

    @classmethod
    def list_all(cls) -> list['RuleDraft']:
        with scoped_session() as session:
            drafts = session.query(cls).order_by(cls.updated_at.desc()).all()
            for draft in drafts:
                session.expunge(draft)
            return drafts

    @classmethod
    def get_one(cls, draft_id: int) -> 'RuleDraft | None':
        with scoped_session() as session:
            draft = session.query(cls).filter(cls.id == draft_id).first()
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
