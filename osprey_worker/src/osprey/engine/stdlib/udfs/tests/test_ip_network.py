from ipaddress import AddressValueError
from typing import Any, Callable, List

import pytest
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.conftest import ExecuteFunction
from osprey.engine.stdlib.udfs.ip_network import IpNetwork
from osprey.engine.stdlib.udfs.json_data import JsonData
from osprey.engine.udf.registry import UDFRegistry

pytestmark: List[Callable[[Any], Any]] = [
    pytest.mark.use_validators([ValidateCallKwargs]),
    pytest.mark.use_udf_registry(UDFRegistry.with_udfs(JsonData, IpNetwork)),
]

sources = """
    UserIp: str = JsonData(path='$.ip', required=False)
    UserIpNetwork = IpNetwork(ip=UserIp)
"""


@pytest.mark.parametrize(
    'ip,expected_result',
    [('127.0.0.1', '127.0.0.0'), ('255.255.255.255', '255.255.255.0'), ('0.0.0.0', '0.0.0.0'), ('0.0.0.1', '0.0.0.0')],
)
def test_accepts_valid_json_data_ip(execute: ExecuteFunction, ip: str, expected_result: str) -> None:
    result = execute(sources, data={'ip': ip})
    assert result == {'UserIp': ip, 'UserIpNetwork': expected_result}


@pytest.mark.parametrize(
    'ip',
    ['123456', '123.456.789.101', '1.1.1.1.1', '1.1.1.256', '0.0.0', '25', '', '127.0.0.foo', 'foo'],
)
def test_invalid_ipv4_failure(execute: ExecuteFunction, ip: str) -> None:
    with pytest.raises(AddressValueError):
        execute(sources, data={'ip': ip})


def test_accepts_json_data_ip_empty(execute: ExecuteFunction) -> None:
    result = execute(sources, data={})
    assert result == {'UserIp': None, 'UserIpNetwork': None}


@pytest.mark.parametrize(
    'ip,expected_result',
    [
        ('090e:d76c:5493:d3be:caaa:1da7:44c9:40e2', '90e:d76c:5493:d3be:caaa:1da7:44c9:40e2'),
        ('2001:0db8:00a0:0000:0000:0000:0000:0001', '2001:db8:a0:0:0:0:0:1'),
    ],
)
def test_valid_ipv6(execute: ExecuteFunction, ip: str, expected_result: str) -> None:
    result = execute(sources, data={'ip': ip})
    assert result == {'UserIp': ip, 'UserIpNetwork': expected_result}


@pytest.mark.parametrize(
    'ip',
    [
        '2001:db8:a0b:12f0::::0:1',
        '2001:1d5::30a::1',
        '?90e:d76c:5493:d3be:caaa:1da7:44c9:40e2',
        '2001:0db8:85a3:0000:0000:8a2e',
        'fe80:2030:31:24',
        '20010:d76c:5493:d3be:caaa:1da7:44c9:40e2',
    ],
)
def test_invalid_ipv6_failure(execute: ExecuteFunction, ip: str) -> None:
    with pytest.raises(AddressValueError):
        execute(sources, data={'ip': ip})
