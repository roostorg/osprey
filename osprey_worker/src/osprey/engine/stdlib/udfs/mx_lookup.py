import gevent
from dns.exception import Timeout as DNSTimeout
from dns.resolver import NXDOMAIN, YXDOMAIN, LRUCache, NoAnswer, NoNameservers, Resolver
from osprey.engine.executor.execution_context import ExpectedUdfException
from osprey.worker.lib.instruments import metrics

from ._prelude import ArgumentsBase, ExecutionContext, UDFBase
from .categories import UdfCategories

# Timeout per DNS query attempt (seconds)
DNS_QUERY_TIMEOUT = 0.5
# Total time allowed for all retries of a single resolve call
DNS_QUERY_LIFETIME = 1.0
# Gevent timeout wrapping the entire execute method (covers both MX + A lookups)
MXLOOKUP_GEVENT_TIMEOUT = 2.0

resolver = Resolver()
resolver.cache = LRUCache(max_size=1000)
resolver.timeout = DNS_QUERY_TIMEOUT
resolver.lifetime = DNS_QUERY_LIFETIME


class Arguments(ArgumentsBase):
    domain: str


class MXLookup(UDFBase[Arguments, str]):
    """Performs an asynchronous MX Record Lookup for a Domain."""

    category = UdfCategories.EMAIL

    execute_async = True

    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> str:
        try:
            with gevent.Timeout(MXLOOKUP_GEVENT_TIMEOUT, False):
                try:
                    mx_answer = resolver.resolve(arguments.domain, 'MX', raise_on_no_answer=True)[0]
                    domain = mx_answer.exchange.to_text()
                    a_record_answers = resolver.resolve(domain, 'A', raise_on_no_answer=True)
                    metrics.increment('mx_lookup.execute.success')
                    #  Sort lexicographically to keep the UDF deterministic
                    return min(answer.to_text() for answer in a_record_answers)
                except DNSTimeout:
                    metrics.increment('mx_lookup.execute.dns_timeout')
                    raise ExpectedUdfException()
                except (NoAnswer, NXDOMAIN, YXDOMAIN, NoNameservers):
                    metrics.increment('mx_lookup.execute.no_record')
                    raise ExpectedUdfException()
        except gevent.Timeout:
            metrics.increment('mx_lookup.execute.gevent_timeout')
            raise ExpectedUdfException()
