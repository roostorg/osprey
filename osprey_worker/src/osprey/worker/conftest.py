from collections.abc import Generator
from typing import Any

import pytest
from osprey.worker.lib.config import Config
from osprey.worker.lib.singletons import CONFIG


# Make Config.configure idempotent for the duration of the test session.
# This prevents "already been bound" errors when multiple fixtures or helpers
# call configure_from_env() within the same interpreter.
@pytest.fixture(scope='session', autouse=True)
def _idempotent_config_configure() -> Generator[None, None, None]:
    original_configure = Config.configure

    def tolerant_configure(self: Config, underlying_config_dict: dict[str, object]) -> None:  # type: ignore[override]
        if getattr(self, '_underlying_config_dict', None) is not None:
            # Already configured: no-op in tests
            return
        return original_configure(self, underlying_config_dict)

    Config.configure = tolerant_configure  # type: ignore[assignment]
    try:
        yield
    finally:
        Config.configure = original_configure  # type: ignore[assignment]


@pytest.fixture(autouse=True)  # autouse = True means automatically use for each test
def config_setup() -> Generator[Any, None, None]:
    CONFIG.instance().configure_from_env()
    # yield is used here to basically split this function into two parts:
    # all code before `yield` is the setup code (run before each test), and
    # all code after `yield` is the teardown code (run after each test)
    yield  # this line is where the testing happens
    # teardown code
    CONFIG.instance().unconfigure_for_tests()
