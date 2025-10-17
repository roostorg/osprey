from contextlib import contextmanager
from typing import Any, Generator

from osprey.engine.language_types.entities import EntityT
from osprey.worker.lib.osprey_shared.labels import EntityLabels
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.lib.storage.labels import LabelsServiceBase
from osprey.worker.lib.storage.postgres import Model, init_from_config, scoped_session
from sqlalchemy import JSON, Column, String, select
from sqlalchemy.dialects.postgresql import insert

logger = get_logger(__name__)


class EntityLabelsModel(Model):
    """SQLAlchemy model for storing entity labels in PostgreSQL"""

    __tablename__ = 'entity_labels'

    entity_key = Column(String, primary_key=True)
    labels = Column(JSON, nullable=False, default={'labels': {}})


class PostgresLabelsService(LabelsServiceBase):
    """
    PostgreSQL-backed implementation of LabelsServiceBase.

    This service stores entity labels in a PostgreSQL database using SQLAlchemy.
    It provides atomic read-modify-write operations through database transactions.
    """

    def __init__(self, database: str = 'osprey_db'):
        """
        Initialize the PostgreSQL labels service.

        Args:
            database: The database name to use. Defaults to 'osprey_db'.
        """
        self.database = database
        # Initialize the database connection from config
        init_from_config(database)
        logger.info(f'Initialized PostgresLabelsService with database: {database}')

    def read_labels(self, entity: EntityT[Any]) -> EntityLabels:
        """
        Read labels for an entity from PostgreSQL.

        Returns an empty EntityLabels if the entity has no labels.
        """
        entity_key = str(entity)

        with scoped_session(database=self.database) as session:
            stmt = select(EntityLabelsModel).where(EntityLabelsModel.entity_key == entity_key)
            result = session.execute(stmt).scalars().first()

            if result is None:
                logger.debug(f'No labels found for entity {entity_key}')
                return EntityLabels()

            labels = EntityLabels.deserialize(result)
            logger.debug(f'Read labels for entity {entity_key}')
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

        with scoped_session(commit=False, database=self.database) as session:
            try:
                # Use SELECT FOR UPDATE to acquire a row-level lock
                stmt = select(EntityLabelsModel).where(EntityLabelsModel.entity_key == entity_key).with_for_update()
                result = session.execute(stmt).scalars().first()

                if result is None:
                    labels = EntityLabels()
                else:
                    labels = EntityLabels.deserialize(result)

                # Yield control - The default LabelsProvider will modify the labels IN PLACE
                yield labels

                # After yield, write the modified labels back
                labels_dict = labels.serialize()
                upsert_stmt = insert(EntityLabelsModel).values(entity_key=entity_key, labels=labels_dict)
                upsert_stmt = upsert_stmt.on_conflict_do_update(index_elements=['entity_key'], set_=labels_dict)
                session.execute(upsert_stmt)

                session.commit()
                logger.debug(f'Committed atomic read-modify-write for entity {entity_key}')

            except Exception:
                session.rollback()
                logger.error(f'Rolled back atomic read-modify-write for entity {entity_key}')
                raise
