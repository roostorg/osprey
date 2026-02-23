import dataclasses
import weakref
from collections.abc import Callable, Sequence
from functools import wraps
from typing import Type, TypeVar

from typing_extensions import Protocol

TypeT = TypeVar('TypeT', bound=type)


def _weakref_inherited(cls: type) -> bool:
    # Taken from:
    # https://github.com/python-attrs/attrs/blob/33b61316f8fd97d78374818e5ecc21068cf69ae3/src/attr/_make.py#L632-L647

    # Traverse the MRO to check for an existing __weakref__.
    for base_cls in cls.__mro__[1:-1]:
        if '__weakref__' in getattr(base_cls, '__dict__', ()):
            return True

    return False


def _add_state_functions(cls_dict: dict[str, object], field_names: Sequence[str]) -> None:
    # Taken from:
    # https://github.com/python-attrs/attrs/blob/33b61316f8fd97d78374818e5ecc21068cf69ae3/src/attr/_make.py#L660-L689

    # __weakref__ is not writable.
    state_attr_names = tuple(name for name in field_names if name != '__weakref__')

    def slots_getstate(self: object) -> Sequence[object]:
        """Automatically created by slotted_dataclass."""
        return tuple(getattr(self, name) for name in state_attr_names)

    def slots_setstate(self: object, state: Sequence[object]) -> None:
        """Automatically created by slotted_dataclass."""
        for name, value in zip(state_attr_names, state):
            object.__setattr__(self, name, value)
            # __bound_setattr(name, value)

    cls_dict['__getstate__'] = slots_getstate
    cls_dict['__setstate__'] = slots_setstate


def add_slots(cls: TypeT) -> TypeT:
    """Adds slots to a dataclass.

    We can't include this in some sort of `slotted_dataclass` wrapper around `dataclassses.dataclass` due to
    https://github.com/python/mypy/issues/5383

    Taken from https://github.com/ericvsmith/dataclasses/blob/master/dataclass_tools.py
    """
    # Need to create a new class, since we can't set __slots__
    #  after a class has been created.

    # Make sure __slots__ isn't already set.
    if '__slots__' in cls.__dict__:
        raise TypeError(f'{cls.__name__} already specifies __slots__')

    # Create a new dict for our new class.
    cls_dict = dict(cls.__dict__)
    field_names = tuple(f.name for f in dataclasses.fields(cls))

    # Some extra things to make sure deepcopy and weakref work.
    _add_state_functions(cls_dict, field_names)
    if '__weakref__' not in field_names and not _weakref_inherited(cls):
        field_names += ('__weakref__',)

    cls_dict['__slots__'] = field_names
    for field_name in field_names:
        # Remove our attributes, if present. They'll still be
        #  available in _MARKER.
        cls_dict.pop(field_name, None)
    # Remove __dict__ itself.
    cls_dict.pop('__dict__', None)
    # And finally create the class.
    qualname = getattr(cls, '__qualname__', None)
    cls = type(cls)(cls.__name__, cls.__bases__, cls_dict)
    if qualname is not None:
        cls.__qualname__ = qualname
    return cls


SelfT = TypeVar('SelfT', contravariant=True)
ReturnT = TypeVar('ReturnT', covariant=True)


# An estimation of what a readonly `@property` looks like. Can be extended to support the rest of the methods that
# descriptors/properties have as necessary.
class Property(Protocol[SelfT, ReturnT]):
    def __get__(self, obj: SelfT, type: Type[SelfT] | None = None) -> ReturnT: ...


def cached_property(func: Callable[[SelfT], ReturnT]) -> Property[SelfT, ReturnT]:
    """Caches a property's value based on the instance."""
    # Like a WeakKeyDictionary, but keyed on instance ID instead of the instance itself (in case the instance
    # isn't hashable).
    value_cache: dict[int, ReturnT] = {}

    @wraps(func)
    def cached_caller(self: SelfT) -> ReturnT:
        self_id = id(self)
        if self_id not in value_cache:
            value_cache[self_id] = func(self)
            weakref.finalize(self, lambda: value_cache.pop(self_id))

        return value_cache[self_id]

    return property(cached_caller)
