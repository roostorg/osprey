from typing import TypeVar

from osprey.engine.language_types.entities import EntityT
from osprey.engine.language_types.osprey_invariant_generic import OspreyInvariantGeneric
from osprey.engine.udf.rvalue_type_checker import EntityTypeChecker, RValueTypeChecker
from osprey.engine.utils.types import cached_property

from . import json_data, json_utils
from ._prelude import ArgumentsBase, ConstExpr, ExecutionContext, UDFBase, ValidationContext
from .categories import UdfCategories

_T = TypeVar('_T')


class EntityArgumentsBase(ArgumentsBase):
    type: ConstExpr[str]
    """The Type of the created Entity."""


class EntityArguments(OspreyInvariantGeneric[_T], EntityArgumentsBase):
    id: _T
    """The value of the created Entity."""


class Entity(OspreyInvariantGeneric[_T], UDFBase[EntityArguments[_T], EntityT[_T]]):
    """Creates an Entity for a given input."""

    category = UdfCategories.ENTITY

    def execute(self, execution_context: ExecutionContext, arguments: EntityArguments[_T]) -> EntityT[_T]:
        return EntityT(arguments.type.value, arguments.id)


# Add in the JsonData arguments
class EntityJsonArguments(EntityArgumentsBase, json_data.Arguments):
    pass


class EntityJson(OspreyInvariantGeneric[_T], UDFBase[EntityJsonArguments, EntityT[_T]]):
    """
    Create an Entity based on the data located at `path`.
    """

    category = UdfCategories.ENTITY

    def __init__(self, validation_context: ValidationContext, arguments: EntityJsonArguments):
        super().__init__(validation_context, arguments)
        self._expr = json_utils.parse_path(arguments.path)

    @cached_property
    def item_type_checker(self) -> RValueTypeChecker:
        assert isinstance(self._rvalue_type_checker, EntityTypeChecker)
        return self._rvalue_type_checker.inner_type

    def execute(self, execution_context: ExecutionContext, arguments: EntityJsonArguments) -> EntityT[_T]:
        value = json_utils.get_from_data(
            self._expr,
            execution_context.get_data(),
            required=arguments.required,
            coerce_type=arguments.coerce_type,
            rvalue_type_checker=self.item_type_checker,
        )

        return EntityT(arguments.type.value, value)
