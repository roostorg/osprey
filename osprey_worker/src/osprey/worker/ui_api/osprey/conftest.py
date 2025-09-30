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


@pytest.fixture(autouse=True, scope='module')
def mock_audit_log_persist():
    with patch('osprey.worker.lib.storage.access_audit_log.AccessAuditLog.persist'):
        yield


@pytest.fixture(autouse=True, scope='module')
def mock_snowflake_generation():
    # Avoid external SNOWFLAKE_API_ENDPOINT dependency in tests
    from osprey.worker.lib.snowflake import Snowflake

    def _gen_one(retries: int = 0) -> Snowflake:  # type: ignore[override]
        return Snowflake(1)

    def _gen_batch(count: int = 1, retries: int = 0) -> list[Snowflake]:  # type: ignore[override]
        return [Snowflake(i + 1) for i in range(count)]

    with (
        patch('osprey.worker.lib.snowflake.generate_snowflake', side_effect=_gen_one),
        patch('osprey.worker.lib.snowflake.generate_snowflake_batch', side_effect=_gen_batch),
    ):
        yield


@pytest.fixture()
def okta_profile_cache():
    # Patch AclConfig.get_abilities_for_user to augment abilities from fake Okta groups
    from osprey.worker.lib.sources_config.subkeys.acl_config import AclConfig

    original = AclConfig.get_abilities_for_user

    def patched(self: AclConfig, user_email: str):  # type: ignore[no-redef]
        abilities = list(original(self, user_email))
        okta_groups = []
        if user_email == 'test_okta@example.com':
            okta_groups = ['App-Osprey-Super-User']
        elif user_email == 'test_okta_not_super@example.com':
            okta_groups = []
        if okta_groups:
            abilities.extend(self.get_abilities_for_okta_groups(okta_groups))
        return abilities

    with patch.object(AclConfig, 'get_abilities_for_user', patched):
        yield
