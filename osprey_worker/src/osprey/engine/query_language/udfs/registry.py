import inspect
import logging
from typing import Any, Type, TypeVar

from osprey.engine.udf.base import QueryUdfBase, UDFBase
from osprey.engine.udf.registry import UDFRegistry

logger = logging.getLogger(__name__)

_UDF = TypeVar('_UDF', bound=UDFBase[Any, Any])


class QueryUdfRegistry(UDFRegistry):
    """A UDFRegistry that only accepts UDFs usable in queries.

    Unlike the general rules UDF registry, every function registered here must subclass
    `QueryUdfBase` (not just `UDFBase`) so it implements `to_druid_query()`. A UDF that doesn't is
    logged and skipped rather than raised: crashing worker startup over one query-only function
    being misconfigured would take down rule execution too, which is a disproportionate blast
    radius for what's a narrow, isolated feature gap (see https://github.com/roostorg/osprey/issues/439).
    Skipping registration means it falls back to the ordinary "unknown function" error if a query
    ever calls it, same as any other unregistered name.
    """

    def __init__(self) -> None:
        super().__init__()
        # Every UDF this registry rejected for not subclassing QueryUdfBase, regardless of where
        # it's defined. test_udf_registry.py asserts this is empty at test time, as a build-time
        # backstop that doesn't depend on every query UDF living in this package's directory -- a
        # convention that register() is the only thing actually enforcing, since nothing stops a UDF
        # defined elsewhere from importing and calling it.
        self.rejected_registrations: list[Type[UDFBase[Any, Any]]] = []

    def register(self, func: Type[_UDF]) -> Type[_UDF]:
        if not issubclass(func, QueryUdfBase):
            reason = 'must subclass QueryUdfBase (and implement to_druid_query()) to be usable in queries'
        elif inspect.isabstract(func):
            # A QueryUdfBase subclass that doesn't implement to_druid_query() (or otherwise leaves an
            # abstract method unimplemented) still passes the issubclass check above, but ValidateCallKwargs
            # would later crash with an uncaught TypeError trying to instantiate it -- the same class of bug
            # this registry exists to prevent, just reached a different way.
            reason = 'is abstract (missing a to_druid_query() implementation) and cannot be used in queries'
        else:
            reason = None

        if reason is not None:
            logger.error(f'{func.__name__} {reason}; skipping registration rather than failing worker startup over it.')
            self.rejected_registrations.append(func)
            return func

        super().register(func)
        return func


UDF_REGISTRY = QueryUdfRegistry()
register = UDF_REGISTRY.register
