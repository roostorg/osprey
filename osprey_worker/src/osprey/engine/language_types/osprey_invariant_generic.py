from typing import Any, Generic, Tuple, TypeVar, Union

_T = TypeVar('_T')


class OspreyInvariantGeneric(Generic[_T]):
    """A special Generic that is allowed within Osprey Rules.

    This generic has a single inner type and that type is invariant, meaning it has to be exactly the same on two
    variants of this class in order for those two variants to be compatible, or one of the two variants has to have
    Any as its inner type.
    """

    # Enforce that any TypeVar used in this class actually is invariant.
    def __class_getitem__(cls, args: Union[type, Tuple[type, ...]]) -> type:
        # Mypy is super unhappy with all this runtime stuff, so lots of `Any`s and ignores
        generic_alias: Any = super().__class_getitem__(args)  # type: ignore # Missing from Generic stub
        params: Tuple[Any, ...] = generic_alias.__parameters__
        if len(params) > 0:
            if len(params) != 1:
                raise TypeError(f'Expected one or zero parameters, got ({params})')
            typevar = params[0]
            if typevar.__bound__ is not None:
                raise TypeError('Cannot provide bound for parameter to OspreyInvariantGeneric')
            if typevar.__constraints__ != ():
                raise TypeError('Cannot provide constraints for parameter to OspreyInvariantGeneric')
            if typevar.__covariant__ is not False:
                raise TypeError('Parameter to OspreyInvariantGeneric must be invariant, cannot be covariant')
            if typevar.__contravariant__ is not False:
                raise TypeError('Parameter to OspreyInvariantGeneric must be invariant, cannot be contravariant')
        return generic_alias
