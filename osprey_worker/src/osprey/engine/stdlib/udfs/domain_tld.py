from typing import cast

from osprey.engine.executor.execution_context import ExpectedUdfException
from tld import get_tld

from ._prelude import ArgumentsBase, ExecutionContext, UDFBase
from .categories import UdfCategories


class Arguments(ArgumentsBase):
    domain: str


class DomainTld(UDFBase[Arguments, str]):
    """Extract the top level domain from a domain name."""

    category = UdfCategories.DNS

    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> str:
        domain_tld = get_tld(arguments.domain, fix_protocol=True, fail_silently=True)
        if domain_tld is None:
            raise ExpectedUdfException()
        return cast(str, domain_tld)  # mypy isn't quite smart enough to infer this
