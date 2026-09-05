import os
import textwrap
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
from psycopg2.errors import DuplicateDatabase
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError
from sqlalchemy_utils import create_database, drop_database

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
    dropping it at teardown would. `_sessions_on_database` checks that condition
    directly instead, which is both stronger and honest about what it is protecting.
    """
    database_name = urlsplit(url).path.lstrip('/')
    return database_name.endswith(_TEST_DATABASE_SUFFIX)


def _sessions_on_database(url: str) -> int:
    """How many sessions other than ours are connected to the database at `url`.

    Asked before dropping, because `sqlalchemy_utils.drop_database` does not fail when
    a database is in use -- for Postgres it runs `pg_terminate_backend` against every
    other session first and then drops it regardless. A second test run would be killed
    silently and fail with connection errors that point nowhere near the cause.
    """
    parts = urlsplit(url)
    maintenance_engine = create_engine(urlunsplit(parts._replace(path='/postgres')), isolation_level='AUTOCOMMIT')
    try:
        with maintenance_engine.connect() as connection:
            return int(
                connection.execute(
                    text('SELECT count(*) FROM pg_stat_activity WHERE datname = :database AND pid <> pg_backend_pid()'),
                    {'database': parts.path.lstrip('/')},
                ).scalar()
                or 0
            )
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
            in_use = _sessions_on_database(url)
            if in_use:
                pytest.fail(
                    f'The test database has {in_use} other session(s) connected, so it is '
                    f'probably in use by another test run. Refusing to drop it.'
                )
            drop_database(url)
            create_database(url)

        postgres.init_from_config('osprey_db')

        config.unconfigure_for_tests()

        yield

        # Always ours to drop by this point: setup either created the database or
        # recreated it, so the only question left is the name.
        if not _is_disposable_database(url):
            return

        try:
            drop_database(url)
        except ProgrammingError as e:
            # Don't fail if the database is already closed
            from psycopg2.errors import InvalidCatalogName

            if not isinstance(e.orig, InvalidCatalogName):
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
