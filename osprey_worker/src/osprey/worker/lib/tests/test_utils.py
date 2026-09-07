import os
import textwrap
import warnings
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING
from urllib.parse import urlsplit, urlunsplit

import pytest
from flask import Flask
from osprey.engine.ast.sources import Sources
from osprey.worker.lib.osprey_engine import bootstrap_engine
from osprey.worker.lib.singletons import CONFIG, ENGINE
from osprey.worker.lib.sources_provider import StaticSourcesProvider
from osprey.worker.lib.sources_publisher import validate_and_push
from osprey.worker.lib.storage import postgres
from psycopg2.errors import DuplicateDatabase, InvalidCatalogName, ObjectInUse
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy_utils import create_database
from sqlalchemy_utils.functions.database import quote

if TYPE_CHECKING:
    from _pytest.config import Config
    from _pytest.fixtures import FixtureRequest


#: Databases this fixture is willing to destroy. Anything not named like this is
#: assumed to belong to a human.
_TEST_DATABASE_SUFFIX = '_test'


def _is_disposable_database(url: str) -> bool:
    """Whether this fixture may destroy the database at `url`.

    This exists because the fixture used to drop whatever `POSTGRES_HOSTS` pointed at,
    and the compose files pointed the tests and the dev stack at the same `osprey`
    database. Running the suite therefore deleted the local development database, and
    the failure surfaced later and elsewhere -- as `osprey-ui-api` refusing to start --
    rather than as a test failure.

    The name is the whole test. An earlier version also required that this run had
    created the database, which was standing in for "somebody else may be using it" --
    but the fixture now recreates an existing database at *setup* to guarantee the
    schema matches the code, and that destroys a database in use just as thoroughly as
    dropping it at teardown would. `_drop_database` asks Postgres to refuse instead,
    which is stronger than either: it is atomic, so there is no window between the
    decision and the destruction.
    """
    database_name = urlsplit(url).path.lstrip('/')
    return database_name.endswith(_TEST_DATABASE_SUFFIX)


def _drop_database(url: str) -> None:
    """Drop the database at `url`, refusing if anything else is connected to it.

    Deliberately not `sqlalchemy_utils.drop_database`: for Postgres that runs
    `pg_terminate_backend` against every other session and then drops the database
    anyway. A second test run would be killed and left failing with connection errors
    that point nowhere near the cause.

    Issuing `DROP DATABASE` ourselves lets Postgres refuse instead, and refuse
    *atomically* -- there is no window between deciding the database is unused and
    destroying it, which a check followed by a drop would leave open.

    The two failure modes arrive as different SQLAlchemy wrappers, which is easy to get
    wrong: in use raises `OperationalError` around `psycopg2.errors.ObjectInUse`, while
    already-gone raises `ProgrammingError` around `InvalidCatalogName`.
    """
    parts = urlsplit(url)
    database = parts.path.lstrip('/')
    maintenance_engine = create_engine(urlunsplit(parts._replace(path='/postgres')), isolation_level='AUTOCOMMIT')
    try:
        with maintenance_engine.connect() as connection:
            connection.execute(text(f'DROP DATABASE {quote(connection, database)}'))
    finally:
        maintenance_engine.dispose()


def make_postgres_database_config_fixture() -> object:
    """Returns a fixture which sets up the Osprey test database for the session.
    Result should be stored in a variable in a conftest.py file.
    """

    @pytest.fixture(scope='session', autouse=True)
    def postgres_database_config() -> Iterator[None]:
        config = CONFIG.instance()
        config.configure_from_env()

        try:
            url = config['POSTGRES_HOSTS']['osprey_db']
        except KeyError:
            url = None

        if url is None:
            pytest.fail('POSTGRES_HOSTS not configured')

        try:
            create_database(url)
        except ProgrammingError as e:
            # `create_database` is asked first and the failure inspected, rather than
            # calling `database_exists`, which is broken:
            # https://github.com/kvesteri/sqlalchemy-utils/issues/472
            if not isinstance(e.orig, DuplicateDatabase):
                raise

            # An existing database is recreated rather than reused. `metadata.create_all`
            # below creates missing tables and never alters existing ones, so reusing one
            # means every schema change since it was made is silently absent -- the tests
            # then pass or fail against a schema nobody chose, and the cause is invisible.
            if not _is_disposable_database(url):
                pytest.fail(
                    f'Refusing to run against an existing database that is not named '
                    f'*{_TEST_DATABASE_SUFFIX}. Point POSTGRES_HOSTS at a test database.'
                )
            try:
                _drop_database(url)
            except OperationalError as drop_error:
                if not isinstance(drop_error.orig, ObjectInUse):
                    raise
                pytest.fail(
                    'The test database is in use, so another test run probably has it. Only '
                    'one run at a time is supported: run-tests.sh pins a single compose '
                    'project, so concurrent runs share the containers as well as this database.'
                )
            create_database(url)

        postgres.init_from_config('osprey_db')

        config.unconfigure_for_tests()

        yield

        # Always ours to drop by this point: setup either created the database or
        # recreated it, so the only question left is the name.
        if not _is_disposable_database(url):
            return

        # Our own pooled connections are connections like any other, and `_drop_database`
        # refuses rather than terminating them, so they have to go first. Whatever is
        # still attached after this belongs to somebody else.
        session_maker = postgres.sessions.get('osprey_db')
        engine = session_maker.kw.get('bind') if session_maker is not None else None
        if engine is not None:
            engine.dispose()

        try:
            _drop_database(url)
        except (OperationalError, ProgrammingError) as e:
            if isinstance(e.orig, InvalidCatalogName):
                return  # already gone; nothing to do
            if isinstance(e.orig, ObjectInUse):
                # Left behind rather than destroyed. The next run recreates it at setup,
                # so this costs nothing but is worth saying out loud.
                warnings.warn(
                    f'Leaving the test database in place: something is still connected to it. {e.orig}',
                    stacklevel=2,
                )
                return
            raise

    return postgres_database_config


def make_app_with_rules_sources_fixture(app_creator: Callable[[], Flask], name: str = 'app') -> object:
    """Returns a fixture which creates the Flask app, with the engine pointing to the test sources. Test sources can
    be set with the `pytest.mark.use_rules_sources` function.
    This function requires that the `add_use_rules_sources` function is called from within a `pytest_configure`
    callback. The result of this function should be stored in a variable in a conftest.py file.
    """

    @pytest.fixture(name=name)
    def app_with_rules_sources(request: 'FixtureRequest') -> Iterator[Flask]:
        os.environ['TESTING'] = 'true'

        rules_source_node = request.node.get_closest_marker('use_rules_sources', default=None)
        if rules_source_node is None:
            sources_to_use = {'main.sml': ''}
        else:
            assert len(rules_source_node.args) == 1
            arg = rules_source_node.args[0]
            if isinstance(arg, dict):
                sources_to_use = arg
            elif isinstance(arg, str):
                sources_to_use = {'main.sml': arg}
            else:
                raise ValueError(f'use_rules_sources only takes a str or Dict[str, str], got {arg!r}')

        sources_to_use = {k: textwrap.dedent(v.rstrip()) for k, v in sources_to_use.items()}
        sources = Sources.from_dict(sources_to_use)
        assert validate_and_push(sources, quiet=True, dry_run=True)
        sources_provider = StaticSourcesProvider(sources)
        engine = bootstrap_engine(sources_provider=sources_provider)

        with ENGINE.override_instance_for_test(engine):
            flask_app = app_creator()
            yield flask_app

    return app_with_rules_sources


def add_use_rules_sources(config: 'Config') -> None:
    """Adds the `pytest.mark.use_rules_sources` function. Should be used in conjunction with
    `make_app_with_rules_sources_fixture`.
    """
    config.addinivalue_line(
        'markers',
        'use_rules_sources(sources_dict_or_str): specifies the content of the rules that should be loaded into the'
        ' osprey engine during the test.',
    )
