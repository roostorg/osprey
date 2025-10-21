# ruff: noqa: E402
from os.path import abspath, dirname
from sys import path

project_root = dirname(dirname(dirname(abspath(__file__))))
path.append(project_root)

from osprey.worker.lib.patcher import patch_all  # isort: skip

patch_all(patch_gevent=False, patch_ddtrace=False)

from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
from osprey.worker.lib.tests import test_utils
from osprey.worker.ui_api.osprey.app import create_app

if TYPE_CHECKING:
    from _pytest.config import Config


app = test_utils.make_app_with_rules_sources_fixture(app_creator=create_app)
postgres_database_config = test_utils.make_postgres_database_config_fixture()


def pytest_configure(config: 'Config') -> None:
    test_utils.add_use_rules_sources(config)


# Override the worker-level config_setup fixture to avoid redundant configuration
# since postgres_database_config already handles Config setup in UI API tests
@pytest.fixture(autouse=True)
def config_setup():
    # No-op: postgres_database_config fixture handles Config setup
    yield


@pytest.fixture(autouse=True, scope='module')
def mock_audit_log_persist():
    with patch('osprey.worker.lib.storage.access_audit_log.AccessAuditLog.persist'):
        yield
