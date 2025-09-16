import re

from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase


class TextContainsArguments(ArgumentsBase):
    text: str
    phrase: str
    case_sensitive = False


class TextContains(UDFBase[TextContainsArguments, bool]):
    def execute(self, execution_context: ExecutionContext, arguments: TextContainsArguments) -> bool:
        escaped = re.escape(arguments.phrase)

        pattern = rf'\b{escaped}\b'

        flags = 0 if arguments.case_sensitive else re.IGNORECASE
        regex = re.compile(pattern, flags)

        return bool(regex.search(arguments.text))
