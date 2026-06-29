import enum
from typing import Any

from osprey.worker.lib.snowflake import Snowflake, generate_snowflake
from sqlalchemy import ARRAY, BigInteger, Column, Text, select
from sqlalchemy.orm import relationship, selectinload
from sqlalchemy.schema import ForeignKey
from sqlalchemy_utils import DateTimeRangeType

from .postgres import Model, scoped_session
from .types import Enum


class SortOrder(enum.Enum):
    DESCENDING = 'DESCENDING'
    ASCENDING = 'ASCENDING'


class Query(Model):
    __tablename__ = 'queries'

    id = Column(BigInteger, primary_key=True)
    parent_id = Column(BigInteger)
    executed_by = Column(Text, nullable=False)
    query_filter = Column(Text, nullable=False)
    date_range = Column(DateTimeRangeType, nullable=False)
    top_n = Column(ARRAY(Text), nullable=False)
    sort_order = Column(Enum(SortOrder, create_type=False), nullable=False)

    def __repr__(self) -> str:
        return f'<Query(id={self.id}, parent_id={self.parent_id})>'

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Query):
            return False

        return (
            other.id == self.id  # type: ignore
            and other.parent_id == self.parent_id
            and other.executed_by == self.executed_by
            and other.query_filter == self.query_filter
            and other.date_range == self.date_range
            and other.top_n == self.top_n
            and other.sort_order == self.sort_order
        )

    @classmethod
    def get_one_with_id(cls, query_id: int) -> Any:
        with scoped_session() as session:
            return session.query(cls).filter(cls.id == query_id).limit(1).first()

    @classmethod
    def get_all(cls, before: int | None = None, limit: int = 100) -> list['Query']:
        table = cls.__table__
        query = table.select().limit(limit).order_by(table.c.id.desc())

        if before is not None:
            query = query.where(table.c.id < before)

        with scoped_session() as session:
            return [cls(**result) for result in session.execute(query)]

    @classmethod
    def get_all_for_user(cls, user_email: str, before: int | None = None, limit: int = 100) -> list['Query']:
        table = cls.__table__
        query = table.select().where(table.c.executed_by == user_email).limit(limit).order_by(table.c.id.desc())

        if before is not None:
            query = query.where(table.c.id < before)

        with scoped_session() as session:
            return session.query(cls).from_statement(query).all()

    @classmethod
    def get_all_user_emails(cls) -> list[str]:
        table = cls.__table__

        with scoped_session() as session:
            emails = session.query(cls.executed_by).distinct(table.c.executed_by).all()
            return [user_email[0] for user_email in emails]

    @classmethod
    def get_all_for_saved_query(cls, query_id: int, limit: int = 10) -> list['Query']:
        table = cls.__table__

        queries_cte = select([table]).where(table.c.id == query_id).cte(recursive=True, name='queries_cte')

        queries_cte = queries_cte.union_all(select([table]).where(table.c.id == queries_cte.c.parent_id))
        query = select([queries_cte]).order_by(queries_cte.c.id.desc()).limit(limit).group_by(queries_cte)

        with scoped_session() as session:
            return [cls(**result) for result in session.execute(query)]

    def insert(self, commit: bool = False) -> None:
        if self.id:
            raise RuntimeError('Cannot insert existing row')

        with scoped_session(commit=commit) as session:
            self.id = generate_snowflake().to_int()
            session.add(self)

    def serialize(self) -> dict[str, Any]:
        assert self.sort_order is not None
        assert self.id is not None
        return {
            'id': str(self.id),
            'parent_id': str(self.parent_id),
            'executed_by': self.executed_by,
            'executed_at': Snowflake(self.id).to_timestamp(),
            'query_filter': self.query_filter,
            'date_range': {'start': self.date_range._lower, 'end': self.date_range._upper},
            'top_n': self.top_n,
            'sort_order': self.sort_order.value,  # type: ignore
        }


class SavedQuery(Model):
    __tablename__ = 'saved_queries'

    id = Column(BigInteger, primary_key=True, nullable=False)
    query_id = Column(BigInteger, ForeignKey('queries.id'), nullable=False)
    name = Column(Text, nullable=False)
    saved_by = Column(Text, nullable=False)

    query = relationship(Query)  # type: ignore

    def __repr__(self) -> str:
        return f'<SavedQuery(id={self.id}, name={self.name})>'

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SavedQuery):
            return False

        return other.id == self.id and other.name == self.name and other.saved_by == self.saved_by

    @classmethod
    def get_one_with_id(cls, saved_query_id: int) -> Any:
        with scoped_session() as session:
            return session.query(cls).filter(cls.id == saved_query_id).limit(1).first()

    @classmethod
    def get_one_with_query_id(cls, query_id: int) -> Any:
        with scoped_session() as session:
            return session.query(cls).filter(cls.query_id == query_id).limit(1).first()

    @classmethod
    def get_all(cls, before: int | None = None, limit: int = 100) -> Any:
        table = cls.__table__
        query = table.select().limit(limit).order_by(table.c.id.desc())

        if before is not None:
            query = query.where(table.c.id < before)

        with scoped_session() as session:
            return session.query(SavedQuery).options(selectinload(SavedQuery.query)).from_statement(query).all()

    @classmethod
    def get_all_for_user(cls, user_email: str, before: int | None = None, limit: int = 100) -> list['Query']:
        table = cls.__table__
        query = table.select().where(table.c.saved_by == user_email).limit(limit).order_by(table.c.id.desc())

        if before is not None:
            query = query.where(table.c.id < before)

        with scoped_session() as session:
            return session.query(cls).from_statement(query).all()

    @classmethod
    def get_all_user_emails(cls) -> list[str]:
        table = cls.__table__

        with scoped_session() as session:
            emails = session.query(cls.saved_by).distinct(table.c.saved_by).all()
            return [email[0] for email in emails]

    def insert(self) -> None:
        if self.id:
            raise RuntimeError('Cannot insert existing row')

        with scoped_session(commit=True) as session:
            self.id = generate_snowflake().to_int()
            session.add(self)

    def update_with_query(self, query: Query) -> None:
        if not self.id:
            raise RuntimeError('Cannot update nonexistent row')
            # Error, can only update already existing saved queries

        with scoped_session(commit=True) as session:
            query.insert()
            self.query_id = query.id
            session.add(self)

    def save(self) -> None:
        with scoped_session(commit=True) as session:
            session.add(self)

    def delete(self) -> None:
        with scoped_session(commit=True) as session:
            session.delete(self)

    def serialize(self) -> dict[str, Any]:
        assert self.id is not None
        return {
            'id': str(self.id),
            'name': self.name,
            'query_id': str(self.query_id),
            'saved_by': self.saved_by,
            'saved_at': Snowflake(self.id).to_timestamp(),
            'query': self.query.serialize(),
        }
