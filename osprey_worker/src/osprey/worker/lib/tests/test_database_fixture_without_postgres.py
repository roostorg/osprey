"""The session database fixture when `POSTGRES_HOSTS` is not set.

Most worker tests never touch Postgres, so a focused `uv run pytest <path>` must run them
without a database. Tests that do need one fail on their own, at first use.
"""

import os
import subprocess
import sys
import textwrap
from pathlib import Path

_CONFTEST = """
from osprey.worker.lib.tests import test_utils

postgres_database_config = test_utils.make_postgres_database_config_fixture()
"""

_TESTS = """
from osprey.worker.lib.storage.postgres import scoped_session


def test_without_database():
    assert True


def test_with_database():
    with scoped_session():
        pass
"""


_ENV_IGNORED = {'POSTGRES_HOSTS', 'PYTEST_ADDOPTS', 'FORCE_COLOR', 'PY_COLORS'}


def _run_inner_pytest(tmp_path: Path, **extra_env: str) -> str:
    (tmp_path / 'conftest.py').write_text(textwrap.dedent(_CONFTEST))
    (tmp_path / 'test_inner.py').write_text(textwrap.dedent(_TESTS))
    env = {key: value for key, value in os.environ.items() if key not in _ENV_IGNORED}
    env.update(extra_env)

    result = subprocess.run(
        [sys.executable, '-m', 'pytest', '-p', 'no:cacheprovider', '--color=no', '-rA', str(tmp_path)],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    return result.stdout


def test_only_tests_that_use_postgres_fail_without_postgres_hosts(tmp_path: Path) -> None:
    output = _run_inner_pytest(tmp_path)

    assert 'PASSED test_inner.py::test_without_database' in output, output
    assert 'FAILED test_inner.py::test_with_database' in output, output
    assert 'POSTGRES_HOSTS is not set' in output, output


def test_postgres_hosts_without_osprey_db_fails_every_test(tmp_path: Path) -> None:
    output = _run_inner_pytest(tmp_path, POSTGRES_HOSTS='{}')

    assert 'ERROR test_inner.py::test_without_database' in output, output
    assert 'ERROR test_inner.py::test_with_database' in output, output
    assert 'POSTGRES_HOSTS has no osprey_db URL' in output, output
