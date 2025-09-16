import socket

IPV4 = socket.AF_INET
IPV6 = socket.AF_INET6


def is_v4(ip_address):
    """
    Helper function to determine if an address is IPv4.

    :param str ip_address: IP Address to check
    :return: True if the IP Address is a valid IPv4 address, False otherwise
    :rtype: bool
    """
    try:
        socket.inet_aton(ip_address)
        return True
    except OSError:
        return False


def is_v6(ip_address):
    """
    Helper function to determine if an address is IPv6

    :param str ip_address: IP Address to check
    :return: True if the IP Address is a valid IPv6 address, False otherwise
    :rtype: bool
    """
    try:
        socket.inet_pton(socket.AF_INET6, ip_address)
        return True
    except OSError:
        return False


def version(ip_address):
    """
    Helper function to determine the version of the IP Address

    This module provides two constants, IPV4 and IPV6, these are actually
    aliases for socket.AF_INET and socket.AF_INET6.  IPV4 and IPV6 are useful
    for semantic checks, while the underlying value is useful if performing
    low level socket operations.

    This operation is more expensive than the simpler is_* checks in this
    module, if you just need to check for a particular version, prefer those.

    :param str ip_address: IP Address to get the version for.
    :return: Version constant if the version can be discerned, None otherwise.
    :rtype: int
    :raises: ValueError if an IP that can not be classified is provided
    """
    if is_v4(ip_address):
        return IPV4
    elif is_v6(ip_address):
        return IPV6
    else:
        raise ValueError(f'Invalid IP Address: {ip_address}')
