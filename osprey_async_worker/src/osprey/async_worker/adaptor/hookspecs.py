"""Hook specifications for the async worker plugin system.

Mirrors osprey.worker.adaptor.hookspecs but uses the 'osprey_async_plugin'
entry_point group. Plugins register async output sinks and UDFs here.

UDFs are shared with the sync worker (they're registered via the existing
'osprey_plugin' hooks and wrapped with SyncUDFAdapter). Async-native UDFs
can also be registered here.

Output sinks MUST be async (AsyncBaseOutputSink) since the async worker
doesn't use gevent.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence, Type

import pluggy
from osprey.engine.ast_validator.base_validator import BaseValidator
from osprey.engine.udf.base import UDFBase

from osprey.async_worker.adaptor.constants import OSPREY_ASYNC_ADAPTOR
from osprey.async_worker.adaptor.interfaces import AsyncBaseOutputSink

if TYPE_CHECKING:
    from osprey.worker.lib.config import Config

hookspec: pluggy.HookspecMarker = pluggy.HookspecMarker(OSPREY_ASYNC_ADAPTOR)


@hookspec
def register_async_output_sinks(config: Config) -> Sequence[AsyncBaseOutputSink]:
    """Register async output sinks for the async worker.

    These must be AsyncBaseOutputSink instances (not sync BaseOutputSink).
    The async worker will call `await sink.push(result)` for each result.
    """
    raise NotImplementedError


@hookspec
def register_udfs() -> Sequence[Type[UDFBase[Any, Any]]]:
    """Register UDFs for the async worker.

    These are the same UDFBase types as the sync worker. The async executor
    runs them in a thread pool via run_in_executor. Async-native UDFs can
    also be registered here in the future.
    """
    raise NotImplementedError


@hookspec
def register_ast_validators() -> Sequence[Type[BaseValidator]]:
    """Register AST validators. Same interface as the sync worker."""
    raise NotImplementedError
