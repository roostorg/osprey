"""First-party async-stdlib plugin.

osprey_async_worker registers itself as an internal pluggy plugin so that
async-native replacements for sync stdlib UDFs (e.g. AsyncMXLookup) flow
through the same `register_udfs` hook used by third-party async plugins.
This keeps `bootstrap_async_udfs` free of hardcoded override lists — adding
a new async stdlib override means appending a class here.

Override-by-class-name is handled by `_deduplicate_udfs` in plugin_manager:
each class returned here shadows any sync stdlib UDF with the same
`__name__`.
"""

from __future__ import annotations

from typing import Any, Sequence, Type

from osprey.engine.udf.base import UDFBase

from osprey.async_worker.adaptor.plugin_manager import hookimpl_osprey_async
from osprey.async_worker.stdlib_udfs.async_mx_lookup import MXLookup


@hookimpl_osprey_async
def register_udfs() -> Sequence[Type[UDFBase[Any, Any]]]:
    return [MXLookup]
