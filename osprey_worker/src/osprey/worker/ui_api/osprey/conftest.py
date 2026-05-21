# ruff: noqa: E402
from os.path import abspath, dirname
from sys import path

project_root = dirname(dirname(dirname(abspath(__file__))))
path.append(project_root)

from osprey.worker.lib.patcher import patch_all  # isort: skip

patch_all(patch_gevent=False, patch_ddtrace=False)

import os
import textwrap
from collections.abc import Iterator
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
from flask import Flask
from osprey.engine.ast.sources import Sources
from osprey.worker.lib.osprey_engine import bootstrap_engine
from osprey.worker.lib.singletons import CONFIG, ENGINE
from osprey.worker.lib.sources_provider import StaticSourcesProvider
from osprey.worker.lib.sources_publisher import validate_and_push
from osprey.worker.lib.tests import test_utils
from osprey.worker.ui_api.osprey.app import create_app

if TYPE_CHECKING:
    from _pytest.config import Config
    from _pytest.fixtures import FixtureRequest


# Custom app fixture that ensures Config is initialized before bootstrap_engine is called
@pytest.fixture(name='app')
def app_with_rules_sources(request: 'FixtureRequest') -> Iterator[Flask]:
    """Flask app fixture that configures Config before creating the engine."""
    # Configure Config first
    CONFIG.instance().configure_from_env()

    try:
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
            flask_app = create_app()
            yield flask_app
    finally:
        # Clean up Config after the test
        CONFIG.instance().unconfigure_for_tests()


postgres_database_config = test_utils.make_postgres_database_config_fixture()


def pytest_configure(config: 'Config') -> None:
    test_utils.add_use_rules_sources(config)


@pytest.fixture(autouse=True, scope='module')
def mock_audit_log_persist():
    with patch('osprey.worker.lib.storage.access_audit_log.AccessAuditLog.persist'):
        yield
