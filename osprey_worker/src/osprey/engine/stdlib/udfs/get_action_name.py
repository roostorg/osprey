from ._prelude import ArgumentsBase, ExecutionContext, UDFBase
from .categories import UdfCategories


class Arguments(ArgumentsBase):
    pass


class GetActionName(UDFBase[Arguments, str]):
    """Returns the Action Name of the event being processed."""

    category = UdfCategories.ENGINE

    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> str:
        return execution_context.get_action_name()
