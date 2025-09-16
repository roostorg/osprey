from dns.resolver import NXDOMAIN, YXDOMAIN, LRUCache, NoAnswer, NoNameservers, Resolver
from osprey.engine.executor.execution_context import ExpectedUdfException

from ._prelude import ArgumentsBase, ExecutionContext, UDFBase
from .categories import UdfCategories

resolver = Resolver()
resolver.cache = LRUCache(max_size=1000)


class Arguments(ArgumentsBase):
    domain: str


class MXLookup(UDFBase[Arguments, str]):
    """Performs an asynchronous MX Record Lookup for a Domain."""

    category = UdfCategories.EMAIL

    execute_async = True

    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> str:
        try:
            mx_answer = resolver.resolve(arguments.domain, 'MX', raise_on_no_answer=True)[0]
            domain = mx_answer.exchange.to_text()
            a_record_answers = resolver.resolve(domain, 'A', raise_on_no_answer=True)
        except (NoAnswer, NXDOMAIN, YXDOMAIN, NoNameservers):
            raise ExpectedUdfException()

        #  Sort lexicographically to keep the UDF deterministic
        return min(answer.to_text() for answer in a_record_answers)
