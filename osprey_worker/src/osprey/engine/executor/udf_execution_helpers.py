from __future__ import annotations

from abc import ABC, abstractmethod
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
    """Indicates that a UDF requires a helper object to execute, which will be provided via the execution context."""

    @classmethod
    @abstractmethod
    def create_provider(cls) -> HelperT:
        """Create an instance of the provider for this UDF."""
        pass


class UDFHelpers:
    """Holds a set of helpers necessary for UDFs to execute, such as an accessor for an external service."""

    def __init__(self) -> None:
        self._helpers: Dict[Type[HasHelperInternal[Any]], object] = {}

    def set_udf_helper(self, udf_class: Type[HasHelperInternal[HelperT]], helper: HelperT) -> 'UDFHelpers':
        self._helpers[udf_class] = helper
        return self

    def get_udf_helper(self, udf: HasHelperInternal[HelperT]) -> HelperT:
        return cast(HelperT, self._helpers[type(udf)])
