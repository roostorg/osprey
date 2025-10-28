import psycogreen.gevent
import sqlalchemy

psycogreen.gevent.patch_psycopg()  # noqa: E402

from contextlib import contextmanager  # noqa: E402
from typing import TYPE_CHECKING, Dict, Iterator, Optional  # noqa: E402

from flask import Flask, has_request_context  # noqa: E402
from osprey.worker.lib.config import Config  # noqa: E402
from osprey.worker.lib.singletons import CONFIG  # noqa: E402
from sqlalchemy import MetaData  # noqa: E402
from sqlalchemy.engine.url import make_url  # noqa: E402
from sqlalchemy.ext.declarative import declarative_base  # noqa: E402
from sqlalchemy.orm import Session, sessionmaker  # noqa: E402
from sqlalchemy.orm.scoping import ThreadLocalRegistry  # type: ignore # missing stub  # noqa: E402

metadata = MetaData()
Model = declarative_base(name='Model', metadata=metadata)

if TYPE_CHECKING:
    SessionMaker = sessionmaker[Session]
else:
    SessionMaker = sessionmaker

sessions: Dict[str, SessionMaker] = {}
session_registries: Dict[str, ThreadLocalRegistry] = {}


def _get_or_init_session(database: str) -> SessionMaker:
    if not sessions.get(database):
        sessions[database] = SessionMaker()
    if not session_registries.get(database):
        session_registries[database] = ThreadLocalRegistry(createfunc=sessions[database])
    return sessions[database]


def init_from_config(database: str) -> None:
    def _init(config: Config) -> None:
        if not config['POSTGRES_HOSTS'].get(database):
            raise Exception(f'Database {database} was not specified in the config!')
        connstr = config['POSTGRES_HOSTS'][database].replace('postgresql://', 'postgresql+psycopg2://')
        Session = _get_or_init_session(database)
        old_engine = Session.kw.get('bind')
        if old_engine:
            if old_engine.url == make_url(connstr):
                return
            old_engine.dispose()
        new_engine = sqlalchemy.create_engine(connstr, pool_pre_ping=True, pool_size=30)
        Session.configure(bind=new_engine)
        # Create all tables defined in the metadata
        metadata.create_all(new_engine)

    CONFIG.instance().register_configuration_callback(_init)


def init_app(app: Flask) -> None:
    @app.teardown_request
    def cleanup_session(_exception: Optional[BaseException]) -> None:
        for session_registry in session_registries.values():
            if session_registry.has():
                session_registry().close()
                session_registry.clear()


@contextmanager
def scoped_session(commit: bool = False, database: str = 'osprey_db') -> Iterator[Session]:
    """Context manager to ensure your session gets closed.
    Safe to nest, the root most level will close the session. Nested calls to this function will implicitly
    use the Session that was created for this thread local's scope.
    When operating within a flask request context, the root most level is considered the request handler,
    and the session will be automatically closed when the request completes.
    :param commit: if True, commits the session when this context manager exits.
    :param database: Returns the session responsible for the specified database. Defaults to 'osprey_db'
    """

    # If this function is being called from within a request context, we don't want to consider ourselves
    # as the root most level. This allows us to lazily create the session on demand, but defer its closure
    # to the request lifecycle.

    if not session_registries.get(database):
        raise Exception(f'Database {database} has not yet been initialized!')
    session_registry: ThreadLocalRegistry = session_registries[database]

    should_close_session = not session_registry.has() and not has_request_context()
    session = session_registry()

    try:
        yield session

        if commit:
            session.commit()
    finally:
        if should_close_session:
            session.close()
            session_registry.clear()
