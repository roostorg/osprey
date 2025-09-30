import os
import textwrap
from typing import TYPE_CHECKING, Callable, Iterator

import pytest
from flask import Flask
from osprey.engine.ast.sources import Sources
from osprey.worker.adaptor.plugin_manager import bootstrap_ast_validators, bootstrap_udfs
from osprey.worker.lib.osprey_engine import OspreyEngine
from osprey.worker.lib.singletons import CONFIG, ENGINE
from osprey.worker.lib.sources_provider import StaticSourcesProvider
from osprey.worker.lib.sources_publisher import validate_and_push
from osprey.worker.lib.storage import postgres
from psycopg2.errors import DuplicateDatabase
from sqlalchemy.exc import ProgrammingError
from sqlalchemy_utils import create_database, drop_database

if TYPE_CHECKING:
    from _pytest.config import Config
    from _pytest.fixtures import FixtureRequest


def make_postgres_database_config_fixture() -> object:
    """Returns a fixture which sets up the Osprey test database for the session.
    Result should be stored in a variable in a conftest.py file.
    If POSTGRES_HOSTS is not configured in the environment, this fixture becomes a no-op.
    """

    @pytest.fixture(scope='session', autouse=True)
    def postgres_database_config() -> Iterator[None]:
        # Only attempt to configure and create the database if POSTGRES_HOSTS is provided
        if 'POSTGRES_HOSTS' not in os.environ:
            # No database configuration available; proceed without setting up Postgres
            yield
            return

        config = CONFIG.instance()
        config.configure_from_env()

        # If POSTGRES_HOSTS is present but does not contain the expected key, no-op
        try:
            url = config['POSTGRES_HOSTS']['osprey']
        except Exception:
            config.unconfigure_for_tests()
            yield
            return

        try:
            create_database(url)
        except ProgrammingError as e:
            # If the database already exists, we're chill and have nothing left to do!
            # This is a workaround to avoid calling `database_exists` as it is currently broken,
            # see: https://github.com/kvesteri/sqlalchemy-utils/issues/472
            if not isinstance(e.orig, DuplicateDatabase):
                raise

        postgres.init_from_config('osprey')

        # Unbind global config so per-test fixtures can bind as needed
        config.unconfigure_for_tests()

        yield

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
        udf_registry, _ = bootstrap_udfs()
        bootstrap_ast_validators()
        engine = OspreyEngine(sources_provider, udf_registry)

        with ENGINE.override_instance_for_test(engine):
            CONFIG.instance().unconfigure_for_tests()
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
