"""Regression tests for plain pytest collection of gevent-patched test modules.

Some test modules under ``osprey_worker`` used to call ``monkey.patch_all`` at
import time. Under plain ``pytest`` (not the pre-patched integration runner)
that late patch mutates global gevent state after threading/ssl are already
imported, raising ``RuntimeError: cannot release un-acquired lock`` during
collection. The ``pytest_sessionfinish`` force-exit hook then saw socket as
patched and called ``os._exit``, hiding the terminal traceback and leaving a
bare exit code 2.

These tests run plain ``pytest --collect-only`` in a child process (so gevent
is NOT pre-patched) and assert collection succeeds with a normal summary.
"""

import re
import subprocess
import sys
from pathlib import Path

import pytest

# repo root is three parents up from this file
_REPO_ROOT = Path(__file__).resolve().parents[2]

# one representative target per import-time gevent patch site:
# - discovery/tests/test_discovery.py patches in the module body
# - instruments/tests/test_concurrency.py patches in the module body
# - etcd/tests/conftest.py patches in the conftest, so collecting any etcd test
#   exercises it
_GEVENT_TEST_MODULES = [
    'osprey_worker/src/osprey/worker/lib/discovery/tests/test_discovery.py',
    'osprey_worker/src/osprey/worker/lib/instruments/tests/test_concurrency.py',
    'osprey_worker/src/osprey/worker/lib/etcd/tests/test_dict.py',
]


def _collect_only_plain(target: str) -> subprocess.CompletedProcess[str]:
    """Run ``pytest --collect-only`` on ``target`` in a plain (non pre-patched) child."""
    return subprocess.run(
        [sys.executable, '-m', 'pytest', '--collect-only', '-q', '-p', 'no:cacheprovider', target],
        cwd=_REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=30,
    )


@pytest.mark.parametrize('target', _GEVENT_TEST_MODULES)
def test_plain_collection_succeeds(target: str) -> None:
    module_path = _REPO_ROOT / target
    assert module_path.exists(), f'{target} does not exist'

    result = _collect_only_plain(target)

    combined = result.stdout + result.stderr
    assert result.returncode == 0, (
        f'plain `pytest --collect-only` on {target} exited {result.returncode}\n'
        f'stdout:\n{result.stdout}\nstderr:\n{result.stderr}'
    )
    # the specific late-monkey-patch failure and pytest's collection-error
    # markers must be absent, and a healthy run reports a nonzero collected
    # count ("no tests collected in ..." must not match)
    assert 'cannot release un-acquired lock' not in combined, combined
    assert 'error during collection' not in combined, combined
    assert re.search(r'\d+ tests? collected in', result.stdout), result.stdout
