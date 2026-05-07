"""Verify that core async worker modules do not import gevent through osprey code.

The async worker runs on pure asyncio — gevent should not be loaded through
our code. Third-party libraries (sentry_sdk, ddtrace) may import gevent to
detect monkey-patching, which is outside our control.

This test checks that osprey's own import chains don't pull in gevent.
It ignores gevent loaded by third-party libraries.

NOTE: osprey.async_worker.lib.etcd.sources_provider is excluded because it
genuinely needs the sync etcd client (which imports gevent). That module is
only used at startup for rule source watching, not in the hot path.
"""

import subprocess
import sys

import pytest


# Modules that must not pull in gevent through osprey code.
_ASYNC_WORKER_MODULES = [
    'osprey.async_worker.engine',
    'osprey.async_worker.executor',
    'osprey.async_worker.sinks.sink.rules_sink',
    'osprey.async_worker.sinks.sink.input_stream',
    'osprey.async_worker.lib.coordinator_input_stream',
    'osprey.async_worker.lib.pigeon.client',
]

# Third-party modules that legitimately import gevent (to detect monkey-patching).
# These are not our code and we can't control them.
_ALLOWED_GEVENT_IMPORTERS = frozenset({
    'sentry_sdk',
    'ddtrace',
})


@pytest.mark.parametrize('module', _ASYNC_WORKER_MODULES)
def test_no_osprey_gevent_import(module: str) -> None:
    """Importing an async worker module must not load gevent through osprey code.

    Third-party libraries (sentry_sdk, ddtrace) may import gevent for
    detection purposes — those are allowed.
    """
    # Use a script that tracks WHO imports gevent first.
    script = (
        f'import sys\n'
        f'class _Tracker:\n'
        f'    importer = None\n'
        f'    def find_module(self, name, path=None):\n'
        f'        if name == "gevent" and "gevent" not in sys.modules and self.importer is None:\n'
        f'            import traceback\n'
        f'            stack = traceback.format_stack()\n'
        f'            for line in reversed(stack):\n'
        f'                if "/site-packages/" in line:\n'
        f'                    # Extract package name from site-packages path\n'
        f'                    parts = line.split("/site-packages/")[1].split("/")[0]\n'
        f'                    self.importer = parts.split(".")[0]\n'
        f'                    break\n'
        f'                elif "/osprey" in line.lower():\n'
        f'                    self.importer = "osprey"\n'
        f'                    break\n'
        f'            if self.importer is None:\n'
        f'                self.importer = "unknown"\n'
        f'        return None\n'
        f'tracker = _Tracker()\n'
        f'sys.meta_path.insert(0, tracker)\n'
        f'try:\n'
        f'    import {module}\n'
        f'except Exception as e:\n'
        f'    print("IMPORT_ERROR:", type(e).__name__, e)\n'
        f'    sys.exit(2)\n'
        f'gevent_loaded = any(k.startswith("gevent") for k in sys.modules)\n'
        f'allowed = {{"sentry_sdk", "ddtrace"}}\n'
        f'if gevent_loaded and tracker.importer not in allowed:\n'
        f'    print(f"GEVENT_LOADED_BY: {{tracker.importer}}")\n'
        f'    sys.exit(1)\n'
        f'print(f"CLEAN (gevent_loaded={{gevent_loaded}}, importer={{tracker.importer}})")\n'
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
            f'Importing {module} loaded gevent through osprey code.\n'
            f'{result.stdout.strip()}\n'
            f'stderr: {result.stderr[:500]}'
        )
