from typing import Dict

from osprey.engine.ast_validator.validation_context import ValidationContext
from osprey.engine.udf.arguments import ArgumentsBase, ConstExpr
from osprey.engine.udf.base import QueryUdfBase
from osprey.rpc.labels.v1.service_pb2 import LabelStatus

from ... import shared_constants
from .registry import register


class Arguments(ArgumentsBase):
    entity_type: ConstExpr[str]
    label_name: ConstExpr[str]


class DidMutateLabel(QueryUdfBase[Arguments, bool]):
    def __init__(self, validation_context: ValidationContext, arguments: Arguments):
        super().__init__(validation_context, arguments)
        self.label_name = arguments.label_name.value
        self.entity_type = arguments.entity_type.value


@register
class DidAddLabel(DidMutateLabel):
    """
    Filters for actions that attempted to add a label

    This does not guarantee that the label was added, or that the entity is
    currently labeled

    # Examples

    `DidAddLabel(entity_type='User',label_name='user_hell_ban')`
    """

    def to_druid_query(self) -> Dict[str, object]:
        return {
            'type': 'like',
            'dimension': shared_constants.ENTITY_LABEL_MUTATION_DIMENSION_NAME,
            'pattern': '%'
            + shared_constants.ENTITY_LABEL_MUTATION_DIMENSION_VALUE(
                self.entity_type, self.label_name, LabelStatus.ADDED
            )
            + '%',
        }


@register
class DidRemoveLabel(DidMutateLabel):
    """
    Filters for actions that attempted to remove a label

    This does not guarantee that the label was removed, or that the entity is not
    currently labeled

    # Examples

    `DidRemoveLabel(entity_type='User',label_name='user_hell_ban')`
    """

    def to_druid_query(self) -> Dict[str, object]:
        return {
            'type': 'like',
            'dimension': shared_constants.ENTITY_LABEL_MUTATION_DIMENSION_NAME,
            'pattern': '%'
            + shared_constants.ENTITY_LABEL_MUTATION_DIMENSION_VALUE(
                self.entity_type, self.label_name, LabelStatus.REMOVED
            )
            + '%',
        }
