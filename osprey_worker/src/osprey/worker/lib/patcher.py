"""Helper to apply necessary patches (gevent, grpc, ddtrace), safe to call multiple times.

This must be imported and called before any other imports in the project.

Example:
```python
from osprey.worker.lib.patcher import patch_all
patch_all()

...other imports...
```
"""

# Importing non-std modules here may break patching
from typing import Any, Dict, Optional


def patch_all(
    patch_gevent: bool = True,
    patch_grpc: bool = True,
    patch_ddtrace: bool = True,
    ddtrace_args: Optional[Dict[str, Any]] = None,
) -> None:
    if patch_gevent:
        # gevent must be patched first https://github.com/DataDog/dd-trace-py/issues/3595
        # The only time we don't patch gevent is tests, where it's patched by the test runner
        import gevent.monkey

        gevent.monkey.patch_all()

    if patch_grpc:
        import grpc.experimental.gevent

        grpc.experimental.gevent.init_gevent()

    if patch_ddtrace:
        import ddtrace

        if ddtrace_args is None:
            ddtrace_args = {}

        ddtrace.patch_all(gevent=True, grpc=patch_grpc, **ddtrace_args)
