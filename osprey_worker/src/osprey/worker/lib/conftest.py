# ruff: noqa: E402
from .patcher import patch_all  # isort: skip

# please ensure this occurs before *any* other imports !
patch_all(patch_gevent=False, patch_ddtrace=False)


from osprey.engine import conftest as rules_conftest  # noqa: E402

from .tests import test_utils  # noqa: E402

postgres_database_config = test_utils.make_postgres_database_config_fixture()

# Take some fixtures from the rules package
execute = rules_conftest.execute
execute_with_result = rules_conftest.execute_with_result
udf_registry = rules_conftest.udf_registry

# Rules-package fixtures used for testing validators

run_validation = rules_conftest.run_validation
check_failure = rules_conftest.check_failure
check_output = rules_conftest.check_output
