from dataclasses import dataclass
from typing import Any, Tuple, Type, TypeVar, Union

import typing_inspect
from osprey.engine.language_types.osprey_invariant_generic import OspreyInvariantGeneric

from .post_execution_convertible import _U, PostExecutionConvertible

_T = TypeVar('_T')


@dataclass(frozen=True, order=True)
class EntityT(OspreyInvariantGeneric[_T], PostExecutionConvertible[_T]):
    type: str
    id: _T

    def __post_init__(self) -> None:
        self._assert_valid_type(type(self.id))

    @classmethod
    def _assert_valid_type(cls, t: Type[Any]) -> None:
        if t not in (str, int):
            raise TypeError(f'Can only have `str` or `int` entities, got `{t.__name__}`')

    def __class_getitem__(cls, args: Union[Type[Any], Tuple[Type[Any], ...]]) -> Type[Any]:
        ret_cls = super().__class_getitem__(args)

        # Because the super method returned we know we have reasonably shaped args
        arg = args[0] if isinstance(args, tuple) else args
        # Check we have a valid type, being a bit more permissive here as opposed to when actually constructed.
        if arg is not Any and type(arg) is not TypeVar:
            cls._assert_valid_type(arg)

        return ret_cls

    def to_post_execution_value(self) -> _T:
        return self.id

    def __str__(self) -> str:
        return f'{self.type}/{self.id}'

    def __repr__(self) -> str:
        return f"EntityT[{type(self.id)}](type='{self.type}', id={self.id})"

    @staticmethod
    def _internal_post_execution_type(cls: Type['PostExecutionConvertible[_U]']) -> Type[_U]:
        # Since we leave PostExecutionConvertible with a generic variable, override how we determine our type to give
        # the real type. Assumes that this has no subclasses.
        return typing_inspect.get_args(cls)[0]

    @classmethod
    def __get_validators__(cls):
        """Pydantic v1 validator"""
        yield cls.validate

    @classmethod
    def validate(cls, v):
        """Validate and convert to EntityT"""
        if isinstance(v, cls):
            return v
        if isinstance(v, dict):
            if 'type' in v and 'id' in v:
                return cls(type=v['type'], id=v['id'])
            raise TypeError(f'EntityT expects dict with "type" and "id" keys, got {v}')
        raise TypeError(f'EntityT expected EntityT or dict; got {type(v)}')


# Make sure this looks as it's used in the Osprey language when used in error messages.
EntityT.__name__ = 'Entity'
