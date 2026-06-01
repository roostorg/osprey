from ._prelude import ArgumentsBase, ExecutionContext, UDFBase
from .categories import UdfCategories


class Arguments(ArgumentsBase):
    pass


class GetActionId(UDFBase[Arguments, int]):
    """Returns the Action ID of the event being processed."""

    category = UdfCategories.ENGINE

    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> int:
        return execution_context.get_action_id()
