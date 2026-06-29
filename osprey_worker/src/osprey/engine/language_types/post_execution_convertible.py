import abc
from typing import Generic, Type, TypeVar

import typing_inspect

_T = TypeVar('_T')
_U = TypeVar('_U')


class PostExecutionConvertible(abc.ABC, Generic[_T]):
    """A mixin that indicates that the value should be converted to a different value if it ends up as a feature."""

    __slots__ = ()

    @abc.abstractmethod
    def to_post_execution_value(self) -> _T:
        """Called on the instance at the end of execution to get the value that should be stored for the feature."""

    # These are static methods so that the implementations can access the generic type. With a classmethod we'd just
    # get `PostExecutionConvertible`, but with the static method we'd get (say) `PostExecutionConvertible[str]`.
    @staticmethod
    def maybe_post_execution_type(t: type) -> type | None:
        origin = typing_inspect.get_origin(t)
        if origin is None:
            origin = t

        # Some things, like `Union`, aren't types, apparently.
        if isinstance(origin, type) and issubclass(origin, PostExecutionConvertible):
            return origin._internal_post_execution_type(t)
        else:
            return None

    @staticmethod
    def _internal_post_execution_type(cls: Type['PostExecutionConvertible[_U]']) -> Type[_U]:
        """What type this class will convert to after execution"""
        # Find this class in the hierarchy
        for parent in typing_inspect.get_generic_bases(cls):
            if typing_inspect.get_origin(parent) is PostExecutionConvertible:
                return typing_inspect.get_args(parent)[0]

        raise TypeError(f'Unable to find conversion type for {cls}')
