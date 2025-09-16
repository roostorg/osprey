from typing import List, cast

from osprey.engine.executor.execution_context import ExpectedUdfException
from tld import get_tld

from ._prelude import ArgumentsBase, ConstExpr, ExecutionContext, UDFBase
from .categories import UdfCategories


class DomainChopperArguments(ArgumentsBase):
    urls: List[str]
    fld: ConstExpr[bool] = ConstExpr.for_default('fld', False)


class DomainChopper(UDFBase[DomainChopperArguments, List[str]]):
    """Parses an list of valid URLs or Domains and returns a requested
    formatting of the Domains or URLs within, chopping the rest."""

    category = UdfCategories.DNS

    @staticmethod
    def _get_fld(domain) -> str:
        normalized_domain = ''

        # extract the TLD from the available domain
        tld = get_tld(domain, fail_silently=True, fix_protocol=True)

        if not tld:
            raise ExpectedUdfException(f'No TLD found for domain {domain}')

        # if tld is present remove it from the remaining hostname
        if tld and tld != domain:
            tld = cast(str, tld)  # mypy isn't quite smart enough to infer this
            without_tld = domain[: -(len(tld) + 1)]
            # create a list of the available hostname and possible subdomains and then fetch the last one
            subdomains = without_tld.split('.')
            hostname = subdomains[-1]
            normalized_domain = f'{hostname}.{tld}'

        # return fld for given domain
        return normalized_domain

    def execute(self, execution_context: ExecutionContext, arguments: DomainChopperArguments) -> List[str]:
        normalized_result = []

        # for list of domains parse out and return the fld + tld only
        if arguments.fld:
            for domain in arguments.urls:
                normalized_result.append(self._get_fld(domain))

        return normalized_result
