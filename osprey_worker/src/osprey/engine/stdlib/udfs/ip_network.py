from ipaddress import AddressValueError, IPv4Address, IPv6Address

from osprey.worker.lib.utils.ip_address import is_v4, is_v6

from ._prelude import ArgumentsBase, ExecutionContext, UDFBase
from .categories import UdfCategories


class Arguments(ArgumentsBase):
    ip: str


class IpNetwork(UDFBase[Arguments, str]):
    """Extracts the IPv4 Network from a provided IPv4 Address by normalizing the fourth octet to zero."""

    """Extracts the IPv6 Network from a provided IPv6 Address by normalizing the leading zeroes"""

    category = UdfCategories.IP

    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> str:
        if is_v4(arguments.ip):
            ipv4_address = IPv4Address(arguments.ip)
            return str(ipv4_address).rsplit('.', 1)[0] + '.0'
        elif is_v6(arguments.ip):
            ipv6_address = IPv6Address(arguments.ip)
            return str(':'.join(format(int(x, 16), 'x') for x in ipv6_address.exploded.split(':')))
        else:
            raise AddressValueError()
