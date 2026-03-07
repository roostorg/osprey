from typing import Any, Dict, List, Optional

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
        payload: Dict[str, Any],
    ) -> None:
        """
        Adds a single `execution_result` to the database
        """
        with scoped_session(commit=True) as session:
            execution_result = PgStoredExecutionResult(id=id, payload=payload)
            session.add(execution_result)

    @classmethod
    def select_one(cls, id: int) -> Optional['PgStoredExecutionResult']:
        """
        Gets stored execution result with id if it exists
        """
        with scoped_session() as session:
            execution_result: Optional['PgStoredExecutionResult'] = (
                session.query(PgStoredExecutionResult).filter(PgStoredExecutionResult.id == id).one_or_none()
            )
            return execution_result

    @classmethod
    def select_many(cls, ids: List[int]) -> List['PgStoredExecutionResult']:
        """
        Gets list of stored execution results with given ids
        """
        with scoped_session() as session:
            execution_results: List['PgStoredExecutionResult'] = (
                session.query(PgStoredExecutionResult).filter(PgStoredExecutionResult.id.in_(ids)).all()
            )
            return execution_results
