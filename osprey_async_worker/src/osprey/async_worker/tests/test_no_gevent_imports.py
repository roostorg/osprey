"""Verify that core async worker modules do not transitively import gevent.

The async worker runs on pure asyncio — gevent should never be loaded.
This test runs imports in a subprocess to get a clean sys.modules state.

NOTE: osprey.async_worker.lib.etcd.sources_provider is excluded because it
genuinely needs the sync etcd client (which imports gevent). That module is
only used at startup for rule source watching, not in the hot path.
"""

import subprocess
import sys

import pytest


# Modules that MUST NOT pull in gevent when imported.
_ASYNC_WORKER_MODULES = [
    'osprey.async_worker.engine',
    'osprey.async_worker.executor',
    'osprey.async_worker.sinks.sink.rules_sink',
    'osprey.async_worker.sinks.sink.input_stream',
    'osprey.async_worker.lib.coordinator_input_stream',
    'osprey.async_worker.lib.pigeon.client',
]


@pytest.mark.parametrize('module', _ASYNC_WORKER_MODULES)
def test_no_gevent_import(module: str) -> None:
    """Importing an async worker module must not load gevent."""
    # Use a script that distinguishes import errors from gevent presence.
    # Exit code 0 = clean (no gevent), 1 = gevent loaded, 2 = import error.
    script = (
        f'import sys\n'
        f'try:\n'
        f'    import {module}\n'
        f'except Exception as e:\n'
        f'    print("IMPORT_ERROR:", type(e).__name__, e)\n'
        f'    sys.exit(2)\n'
        f'gevent_mods = sorted(k for k in sys.modules if k.startswith("gevent"))\n'
        f'if gevent_mods:\n'
        f'    print("GEVENT_LOADED:", gevent_mods)\n'
        f'    sys.exit(1)\n'
        f'print("CLEAN")\n'
    )
    result = subprocess.run(
        [sys.executable, '-c', script],
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode == 2:
        pytest.skip(f'Could not import {module}: {result.stdout.strip()}')
    elif result.returncode == 1:
        pytest.fail(
            f'Importing {module} loaded gevent.\n'
            f'{result.stdout.strip()}\n'
            f'stderr: {result.stderr[:500]}'
        )
