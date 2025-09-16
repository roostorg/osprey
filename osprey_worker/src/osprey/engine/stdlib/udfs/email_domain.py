from typing import cast

from osprey.engine.executor.execution_context import ExpectedUdfException
from tld import get_tld

from ._prelude import ArgumentsBase, ExecutionContext, UDFBase
from .categories import UdfCategories


class Arguments(ArgumentsBase):
    email: str


class EmailDomain(UDFBase[Arguments, str]):
    """Extracts the Domain from an email address."""

    category = UdfCategories.EMAIL

    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> str:
        split_email = arguments.email.rsplit('@', 1)
        if len(split_email) != 2:
            raise ExpectedUdfException()

        normalized_domain = split_email[1].lower()
        if not normalized_domain:
            raise ExpectedUdfException()

        tld = get_tld(normalized_domain, fail_silently=True, fix_protocol=True)
        if tld is not None and tld != normalized_domain:
            tld = cast(str, tld)  # mypy isn't quite smart enough to infer this
            without_tld = normalized_domain[: -(len(tld) + 1)]
            top_subdomain = without_tld.split('.')[-1]
            normalized_domain = f'{top_subdomain}.{tld}'

        return normalized_domain


class EmailSubdomain(UDFBase[Arguments, str]):
    """Extracts the Domain with subdomains from an email address."""

    category = UdfCategories.EMAIL

    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> str:
        split_email = arguments.email.rsplit('@', 1)

        if len(split_email) != 2 or not split_email[1]:
            raise ExpectedUdfException()

        normalized_domain = split_email[1].lower()

        return normalized_domain
