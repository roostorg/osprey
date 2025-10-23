from contextlib import contextmanager
from typing import Any, Generator

from osprey.engine.language_types.entities import EntityT
from osprey.worker.lib.osprey_shared.labels import EntityLabels
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.lib.storage.labels import LabelsServiceBase
from osprey.worker.lib.storage.postgres import Model, init_from_config, scoped_session
from sqlalchemy import Column, String, select
from sqlalchemy.dialects.postgresql import JSONB, insert

logger = get_logger(__name__)


class EntityLabelsModel(Model):
    """SQLAlchemy model for storing entity labels in PostgreSQL"""

    __tablename__ = 'entity_labels'

    entity_key = Column(String, primary_key=True)
    labels = Column(JSONB, nullable=False)

    def __str__(self) -> str:
        return f'EntityLabelsModel(entity_key={self.entity_key}, labels={self.labels})'


class PostgresLabelsService(LabelsServiceBase):
    """
    PostgreSQL-backed implementation of LabelsServiceBase.

    This service stores entity labels in a PostgreSQL database using SQLAlchemy.
    It provides atomic read-modify-write operations through database transactions.
    """

    def __init__(self, database: str = 'osprey_db') -> None:
        """
        Initialize the PostgreSQL labels service.
        Note: This will not init the postgres connection; To do that,
        initialize() must be called (which is called by the LabelsProvider
        by default)

        Args:
            database: The database name to use. Defaults to 'osprey_db'.
        """
        super().__init__()
        self._database_name: str = database

    def initialize(self) -> None:
        init_from_config(self._database_name)
        logger.info(f'Initialized PostgresLabelsService with database: {self._database_name}')

    def read_labels(self, entity: EntityT[Any]) -> EntityLabels:
        """
        Read labels for an entity from PostgreSQL.

        Returns an empty EntityLabels if the entity has no labels.
        """
        entity_key = str(entity)

        with scoped_session(database=self._database_name) as session:
            stmt = select(EntityLabelsModel).where(EntityLabelsModel.entity_key == entity_key)
            result = session.scalars(stmt).first()

            if result is None:
                logger.debug(f'No labels found for entity {entity_key}')
                return EntityLabels()

            labels = EntityLabels.deserialize(result.labels)
            logger.debug(f'Read labels for entity {entity_key}', result)
            return labels

    @contextmanager
    def read_modify_write_labels_atomically(self, entity: EntityT[Any]) -> Generator[EntityLabels, None, None]:
        """
        Context manager for atomic read-modify-write operations.

        This context manager:
        1. Opens a database transaction
        2. Acquires a row-level lock using SELECT FOR UPDATE
        3. Reads and returns the current labels
        4. Yields control to the caller (LabelsProvider)
        5. The caller modifies the labels IN PLACE
        6. On exit, writes the modified labels and commits the transaction

        The key insight: The caller modifies the yielded labels object directly,
        and this context manager persists those changes atomically.

        For systems that don't need locking (e.g., in-memory stores), this can
        be simplified to:
        ```py
        labels = self.read_labels(entity)
        yield labels
        # write the labels here
        """
        entity_key = str(entity)

        with scoped_session(commit=False, database=self._database_name) as session:
            try:
                # Use SELECT FOR UPDATE to acquire a row-level lock
                stmt = select(EntityLabelsModel).where(EntityLabelsModel.entity_key == entity_key).with_for_update()
                result = session.scalars(stmt).first()

                if result is None:
                    labels = EntityLabels()
                else:
                    labels = EntityLabels.deserialize(result.labels)

                # Yield control - The default LabelsProvider will modify the labels IN PLACE
                yield labels

                # After yield, write the modified labels back
                labels_dict = labels.serialize()
                upsert_stmt = insert(EntityLabelsModel).values(entity_key=entity_key, labels=labels_dict)
                upsert_stmt = upsert_stmt.on_conflict_do_update(
                    index_elements=['entity_key'], set_={EntityLabelsModel.labels: labels_dict}
                )
                session.execute(upsert_stmt)

                session.commit()
                logger.debug(f'Committed atomic read-modify-write for entity {entity_key}', labels_dict)

            except Exception:
                session.rollback()
                logger.error(f'Rolled back atomic read-modify-write for entity {entity_key}')
                raise
