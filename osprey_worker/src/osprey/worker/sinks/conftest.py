# ruff: noqa: E402
from osprey.worker.lib.patcher import patch_all

patch_all(patch_gevent=False, patch_ddtrace=False)  # please ensure this occurs before *any* other imports !


from osprey.worker.lib.tests import test_utils

postgres_database_config = test_utils.make_postgres_database_config_fixture()
