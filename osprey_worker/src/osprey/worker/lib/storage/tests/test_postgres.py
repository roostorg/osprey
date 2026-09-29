import threading

import sqlalchemy
from osprey.worker.lib.singletons import CONFIG
from osprey.worker.lib.storage import postgres
from psycopg2.errors import DuplicateDatabase, InvalidCatalogName
from sqlalchemy.exc import ProgrammingError
from sqlalchemy_utils import create_database, drop_database


def test_create_schema_survives_concurrent_callers_on_a_fresh_database():
    """Regression test for issue #432: on a fresh database, two processes (e.g. the worker and
    the UI API) both calling create_schema() at startup used to be able to race on `CREATE TYPE`
    for the job_status enum, since SQLAlchemy's enum creation is check-then-create rather than
    atomic. Both would see the type as missing, both would issue CREATE TYPE, and the loser would
    crash with a UniqueViolation on `pg_type_typname_nsp_index`.

    Simulate two racing processes with two independent engines hitting a brand new database at
    the same time, and assert neither raises.
    """
    base_url = CONFIG.instance()['POSTGRES_HOSTS']['osprey_db']
    fresh_url = base_url.rsplit('/', 1)[0] + '/osprey_test_concurrent_schema_create'

    try:
        drop_database(fresh_url)
    except ProgrammingError as e:
        if not isinstance(e.orig, InvalidCatalogName):
            raise
    try:
        create_database(fresh_url)
    except ProgrammingError as e:
        if not isinstance(e.orig, DuplicateDatabase):
            raise

    try:
        errors: list[Exception] = []
        barrier = threading.Barrier(2)

        def _create_schema() -> None:
            engine = sqlalchemy.create_engine(fresh_url)
            try:
                barrier.wait(timeout=5)
                postgres.create_schema(engine)
            except Exception as e:
                errors.append(e)
            finally:
                engine.dispose()

        threads = [threading.Thread(target=_create_schema) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)

        assert not errors, errors
    finally:
        drop_database(fresh_url)
