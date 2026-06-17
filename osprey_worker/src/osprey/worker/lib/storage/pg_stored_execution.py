from __future__ import annotations

from typing import Any

from sqlalchemy import BigInteger, Column
from sqlalchemy.dialects.postgresql import JSONB

from .postgres import Model, scoped_session


class PgStoredExecutionResult(Model):
    """
    `stored_execution_result` stores results as jsonb
    """

    __tablename__ = 'stored_execution_result'

    id = Column(BigInteger, primary_key=True)
    payload = Column(JSONB, nullable=False)

    @classmethod
    def insert(
        cls,
        id: int,
        payload: dict[str, Any],
    ) -> None:
        """
        Adds a single `execution_result` to the database
        """
        with scoped_session(commit=True) as session:
            execution_result = PgStoredExecutionResult(id=id, payload=payload)
            session.add(execution_result)

    @classmethod
    def select_one(cls, id: int) -> 'PgStoredExecutionResult' | None:
        """
        Gets stored execution result with id if it exists
        """
        with scoped_session() as session:
            execution_result: 'PgStoredExecutionResult' | None = (
                session.query(PgStoredExecutionResult).filter(PgStoredExecutionResult.id == id).one_or_none()
            )
            return execution_result

    @classmethod
    def select_many(cls, ids: list[int]) -> list['PgStoredExecutionResult']:
        """
        Gets list of stored execution results with given ids
        """
        with scoped_session() as session:
            execution_results: list['PgStoredExecutionResult'] = (
                session.query(PgStoredExecutionResult).filter(PgStoredExecutionResult.id.in_(ids)).all()
            )
            return execution_results
