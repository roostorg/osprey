from concurrent import futures

import grpc
import pytest
from osprey.rpc.pigeon.tests.v1.tests_pb2 import ScalarRoutingValueRequest, ScalarRoutingValueResponse
from osprey.rpc.pigeon.tests.v1.tests_pb2_grpc import (
    TestServiceServicer,
    TestServiceStub,
    add_TestServiceServicer_to_server,
)
from osprey.worker.lib.discovery.exceptions import ServiceUnavailable
from osprey.worker.lib.discovery.service import Service
from osprey.worker.lib.pigeon.client import RoutedClient, RoutingType, UnaryUnaryRpcCallable
from osprey.worker.lib.pigeon.exceptions import RPCException

S1_VALID_ROUTING_VALUE = 1000
S2_VALID_ROUTING_VALUE = 2000
INVALID_ROUTING_VALUE = 9999


class TestService1(TestServiceServicer):
    __test__ = True

    def ScalarRoutingValue(self, request, context):
        if request.routing_value == S1_VALID_ROUTING_VALUE:
            return ScalarRoutingValueResponse(routing_value=request.routing_value)

        context.set_code(grpc.StatusCode.NOT_FOUND)
        context.set_details('Not found')


class TestService2(TestServiceServicer):
    __test__ = True

    def ScalarRoutingValue(self, request, context):
        if request.routing_value == S2_VALID_ROUTING_VALUE:
            return ScalarRoutingValueResponse(routing_value=request.routing_value)

        context.set_code(grpc.StatusCode.NOT_FOUND)
        context.set_details('Not found')


REQUEST_FIELD_SCALAR = 'routing_value'

SERVICE_NAME = 'pigeon_test_service'
SERVICE_1_PORT = 50053
SERVICE_2_PORT = 50054

SERVICE_1 = Service(SERVICE_NAME, SERVICE_1_PORT, ports={'grpc': SERVICE_1_PORT})
SERVICE_2 = Service(SERVICE_NAME, SERVICE_2_PORT, ports={'grpc': SERVICE_2_PORT})


def test_find_in_primary(client):
    request = ScalarRoutingValueRequest(routing_value=S1_VALID_ROUTING_VALUE)
    rpc_callables = [client.ScalarRoutingValue]
    for rpc_callable in rpc_callables:
        retry_policy = {
            'max_secondaries_to_retry': 0,
            'retryable_grpc_status_codes': {grpc.StatusCode.NOT_FOUND},
        }

        response = rpc_callable(request, retry_policy=retry_policy)
        assert response.routing_value == S1_VALID_ROUTING_VALUE


def test_skip_primary_find_in_secondary(client):
    request = ScalarRoutingValueRequest(routing_value=S2_VALID_ROUTING_VALUE)
    rpc_callables = [client.ScalarRoutingValue]
    for rpc_callable in rpc_callables:
        retry_policy = {
            'max_secondaries_to_retry': 0,
            'retryable_grpc_status_codes': {grpc.StatusCode.NOT_FOUND},
        }

        with pytest.raises(RPCException):
            rpc_callable(request, retry_policy=retry_policy)

        retry_policy['max_secondaries_to_retry'] = 1
        response = rpc_callable(request, retry_policy=retry_policy)
        assert response.routing_value == S2_VALID_ROUTING_VALUE


def test_not_found_in_any(mocker, client):
    request = ScalarRoutingValueRequest(routing_value=INVALID_ROUTING_VALUE)
    rpc_callables = [client.ScalarRoutingValue]
    spy = mocker.spy(UnaryUnaryRpcCallable, 'request')
    for rpc_callable in rpc_callables:
        retry_policy = {
            'max_secondaries_to_retry': 0,
            'retryable_grpc_status_codes': {grpc.StatusCode.NOT_FOUND},
        }

        with pytest.raises(RPCException):
            rpc_callable(request, retry_policy=retry_policy)

        mocker.resetall()
        retry_policy['max_secondaries_to_retry'] = 1
        with pytest.raises(RPCException):
            rpc_callable(request, retry_policy=retry_policy)
        assert spy.call_count == 2

        mocker.resetall()
        retry_policy['max_secondaries_to_retry'] = 2
        with pytest.raises(RPCException):
            rpc_callable(request, retry_policy=retry_policy)
        assert spy.call_count == 3

        # Ensure we don't loop additional times after service not found
        mocker.resetall()
        retry_policy['max_secondaries_to_retry'] = 3
        with pytest.raises(RPCException):
            rpc_callable(request, retry_policy=retry_policy)
        assert spy.call_count == 3


def run_server(port, service):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    add_TestServiceServicer_to_server(service, server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    return server


@pytest.fixture(scope='session', autouse=True)
def run_servers(request):
    servers = [run_server(SERVICE_1_PORT, TestService1()), run_server(SERVICE_2_PORT, TestService2())]

    def shutdown_servers():
        handlers = [server.stop(1) for server in servers]
        for handler in handlers:
            handler.wait()

    request.addfinalizer(shutdown_servers)


@pytest.fixture(scope='function', autouse=True)
def mock_select(mocker):
    def mock_select_service(_self, routing_value=None, secondaries=None, instances_to_skip=0):
        if instances_to_skip == 0:
            return SERVICE_1
        elif instances_to_skip == 1:
            return SERVICE_2
        else:
            raise ServiceUnavailable('failed during test')

    mocker.patch('osprey.worker.lib.discovery.service_watcher.ServiceWatcher.select', mock_select_service)


@pytest.fixture()
def client() -> RoutedClient[TestServiceStub]:
    return RoutedClient(
        SERVICE_NAME,
        stub_cls=TestServiceStub,
        request_field=REQUEST_FIELD_SCALAR,
        routing_type=RoutingType.SCALAR,
        read_timeout=5.0,
    )
