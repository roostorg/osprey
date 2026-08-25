"""Root pytest configuration for osprey_worker package.

This conftest.py is discovered by pytest during plugin loading,
ensuring that custom pytest options and markers are registered before
argument parsing occurs. Placing this at the osprey_worker level ensures
it's loaded regardless of whether pytest is invoked from the worktree root
or from within osprey_worker/ subpaths.
"""

from typing import TYPE_CHECKING

import pytest
from gevent import monkey

if TYPE_CHECKING:
    from _pytest.config import Config
    from _pytest.config.argparsing import Parser


# capture launch state before test imports can patch gevent
_STARTED_PREPATCHED = monkey.is_module_patched('socket')


def pytest_addoption(parser: 'Parser') -> None:
    """Register custom pytest options.

    This hook must be discovered early by pytest during plugin loading
    (before argument parsing).
    """
    parser.addoption(
        '--write-outputs', action='store_true', help='write checked validator outputs instead of checking them'
    )


def pytest_configure(config: 'Config') -> None:
    """Register custom pytest markers.

    This hook must be discovered early by pytest during plugin loading.
    """
    config.addinivalue_line(
        'markers',
        'use_validators([validator_classes, ...]): used with the `run_validation` fixture, '
        'to specify the validators to execute.',
    )
    config.addinivalue_line(
        'markers',
        'use_standard_rules_validators(): used with the `run_validation` fixture, runs all normal rules validators.',
    )
    config.addinivalue_line(
        'markers',
        'inject_validator_result(validator={...}, result={...}): used with the `run_validation` fixture, '
        'to mock requisite validator results.',
    )
    config.addinivalue_line(
        'markers', 'use_udf_registry(UDFRegistry()): use a given udf registry during validation/execution'
    )
    config.addinivalue_line(
        'markers', 'use_osprey_stdlib(): uses the osprey udf stdlib registry during validation/execution.'
    )
    config.addinivalue_line(
        'markers',
        'vary_output_by_py_version(): instructs the `check_output` feature to record a separate file for '
        'different python major/minor versions',
    )


@pytest.hookimpl(trylast=True)
def pytest_sessionfinish(session: object, exitstatus: int) -> None:
    """Force-exit the gevent-monkey-patched test process once the run is done.

    The test runner launches pytest under gevent monkey-patching
    (``python -m gevent.monkey --module pytest``). After the suite finishes,
    the process can stall inside gevent's interpreter finalization and never
    exit, so the integration-tests CI job hangs until its 30-minute timeout.
    Exit immediately here (trylast, so this runs after the junit/terminal
    hooks) to skip that teardown. Guarded on pytest having *started* under the
    gevent-monkey runner (see ``_STARTED_PREPATCHED``) so a plain ``pytest`` run
    finalizes normally even if a test module monkey-patched during collection.
    """
    import os
    import sys

    if not _STARTED_PREPATCHED:
        return
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(exitstatus)
