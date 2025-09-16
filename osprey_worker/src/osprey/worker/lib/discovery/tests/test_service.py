from __future__ import annotations

import pytest
from osprey.worker.lib.discovery.service import Service


@pytest.fixture
def basic_service():
    return Service('name', 1234, 'localhost', None, None, '127.0.0.2')


@pytest.fixture
def noip_service():
    return Service('name', 1234, 'localhost', None, None, None)


def test_id(basic_service: Service) -> None:
    # Needed for compatibility with routing ring assumptions
    assert basic_service.id == 'localhost:1234'


def test_conn_address(basic_service: Service) -> None:
    assert basic_service.connection_address == '127.0.0.2'


def test_no_ip_conn_address(noip_service: Service) -> None:
    assert noip_service.connection_address == 'localhost'


def test_serialize_loop(basic_service: Service) -> None:
    assert Service.deserialize(basic_service.serialize()) == basic_service


def test_serialize_loop_no_ip(noip_service: Service) -> None:
    assert Service.deserialize(noip_service.serialize()) == noip_service
