# ruff: noqa: E402
from osprey.worker.lib.patcher import patch_all

patch_all(patch_gevent=False, patch_ddtrace=False)  # please ensure this occurs before *any* other imports !

from typing import Any, Generator

import pytest
from osprey.worker.lib.singletons import CONFIG
from osprey.worker.lib.tests import test_utils

postgres_database_config = test_utils.make_postgres_database_config_fixture()


@pytest.fixture(autouse=True)  # autouse = True means automatically use for each test
def config_setup() -> Generator[Any, None, None]:
    CONFIG.instance().configure_from_env()
    # yield is used here to basically split this function into two parts:
    # all code before `yield` is the setup code (run before each test), and
    # all code after `yield` is the teardown code (run after each test)
    yield  # this line is where the testing happens
    # teardown code
    CONFIG.instance().unconfigure_for_tests()
