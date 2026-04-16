from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Callable, Dict, Generic, Hashable, Type, TypeVar, cast

from osprey.engine.executor.external_service_utils_base import ExternalService

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
        accessor = execution_context.get_external_service_accessor(provider)
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
        self._helper_factories: Dict[Type[HasHelperInternal[Any]], Callable[[], object]] = {}

    def set_udf_helper(self, udf_class: Type[HasHelperInternal[HelperT]], helper: HelperT) -> 'UDFHelpers':
        self._helpers[udf_class] = helper
        self._helper_factories.pop(udf_class, None)
        return self

    def set_udf_helper_factory(
        self,
        udf_class: Type[HasHelperInternal[HelperT]],
        helper_factory: Callable[[], HelperT],
    ) -> 'UDFHelpers':
        self._helper_factories[udf_class] = helper_factory
        self._helpers.pop(udf_class, None)
        return self

    def get_udf_helper(self, udf: HasHelperInternal[HelperT]) -> HelperT:
        udf_class = type(udf)
        if udf_class not in self._helpers:
            self._helpers[udf_class] = self._helper_factories[udf_class]()
        return cast(HelperT, self._helpers[udf_class])
