from __future__ import annotations

from abc import ABC
from typing import TYPE_CHECKING, Any, Dict, Generic, Hashable, Type, TypeVar, cast

from osprey.engine.executor.external_service_utils import (
    ExternalService,
    ExternalServiceAccessor,
)

if TYPE_CHECKING:
    from osprey.engine.executor.execution_context import ExecutionContext
HelperT = TypeVar('HelperT')
KeyT = TypeVar('KeyT', bound=Hashable)
ValueT = TypeVar('ValueT')


class HasHelperInternal(Generic[HelperT]):
    """Internal HasHelper that can be used for artisinally created providers.
    Not intended to be used directly except for some .
    """

    def accessor_get(self, execution_context: ExecutionContext, key: KeyT, lock: bool = False) -> ValueT:  # type: ignore[type-var]
        udf = cast(HasHelperInternal[ExternalService[KeyT, ValueT]], self)
        provider: ExternalService[KeyT, ValueT] = execution_context.get_udf_helper(udf)
        accessor: ExternalServiceAccessor[KeyT, ValueT] = execution_context.get_external_service_accessor(provider)
        return accessor.get(key)


class HasHelper(HasHelperInternal[HelperT], ABC):
    """Indicates that a UDF requires a helper object to execute, which will be provided via the execution context.

    Subclasses may optionally implement `create_provider` to allow automatic helper bootstrapping via plugins.
    Tests or callers can also inject helpers explicitly using `UDFHelpers.set_udf_helper` without requiring this method.
    """

    @classmethod
    def create_provider(cls) -> HelperT:
        """Create an instance of the provider for this UDF.

        Default behavior raises to signal that automatic provider creation isn't available. This keeps subclasses
        instantiable for validation while still requiring explicit opt-in for plugin bootstrapping.
        """
        raise NotImplementedError(
            f'{cls.__name__} does not define create_provider(); either implement it or inject via UDFHelpers'
        )


class UDFHelpers:
    """Holds a set of helpers necessary for UDFs to execute, such as an accessor for an external service."""

    def __init__(self) -> None:
        self._helpers: Dict[Type[HasHelperInternal[Any]], object] = {}

    def set_udf_helper(self, udf_class: Type[HasHelperInternal[HelperT]], helper: HelperT) -> 'UDFHelpers':
        self._helpers[udf_class] = helper
        return self

    def get_udf_helper(self, udf: HasHelperInternal[HelperT]) -> HelperT:
        return cast(HelperT, self._helpers[type(udf)])
