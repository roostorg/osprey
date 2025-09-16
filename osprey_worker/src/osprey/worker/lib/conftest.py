# ruff: noqa: E402
from .patcher import patch_all  # isort: skip

# please ensure this occurs before *any* other imports !
patch_all(patch_gevent=False, patch_ddtrace=False)

from typing import Any, Generator  # noqa: E402

import pytest  # noqa: E402
from osprey.engine import conftest as rules_conftest  # noqa: E402
from osprey.worker.lib.singletons import CONFIG  # noqa: E402

from .tests import test_utils  # noqa: E402

postgres_database_config = test_utils.make_postgres_database_config_fixture()

# Take some fixtures from the rules package
execute = rules_conftest.execute
execute_with_result = rules_conftest.execute_with_result
udf_registry = rules_conftest.udf_registry

# Rules-package fixtures used for testing validators
from _pytest.config.argparsing import Parser


@pytest.fixture(autouse=True)  # autouse = True means automatically use for each test
def config_setup() -> Generator[Any, None, None]:
    CONFIG.instance().configure_from_env()
    # yield is used here to basically split this function into two parts:
    # all code before `yield` is the setup code (run before each test), and
    # all code after `yield` is the teardown code (run after each test)
    yield  # this line is where the testing happens
    # teardown code
    CONFIG.instance().unconfigure_for_tests()


def pytest_addoption(parser: Parser) -> None:
    parser.addoption(
        '--write-outputs', action='store_true', help='write checked validator outputs instead of checking them'
    )


run_validation = rules_conftest.run_validation
check_failure = rules_conftest.check_failure
check_output = rules_conftest.check_output
