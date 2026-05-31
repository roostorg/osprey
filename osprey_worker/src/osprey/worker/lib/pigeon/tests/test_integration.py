from concurrent import futures

import grpc
import pytest
from osprey.rpc.pigeon.tests.v1.tests_pb2 import (
    Foo,
    ListRoutingValueRequest,
    ListRoutingValueResponse,
    MapMessageRoutingValueRequest,
    MapMessageRoutingValueResponse,
    MapRoutingValueRequest,
    MapRoutingValueResponse,
    ScalarRoutingValueRequest,
    ScalarRoutingValueResponse,
)
from osprey.rpc.pigeon.tests.v1.tests_pb2_grpc import (
    TestServiceServicer,
    TestServiceStub,
    add_TestServiceServicer_to_server,
)
from osprey.worker.lib.discovery.service import Service
from osprey.worker.lib.pigeon.client import (
    InvalidRoutingValueException,
    RoutedClient,
    RoutingType,
)


class TestService(TestServiceServicer):
    __test__ = False

    def ScalarRoutingValue(self, request, _context):
        return ScalarRoutingValueResponse(routing_value=request.routing_value)

    def ListRoutingValue(self, request, _context):
        return ListRoutingValueResponse(routing_values=request.routing_values)

    def MapRoutingValue(self, request, _context):
        return MapRoutingValueResponse(routing_values=request.routing_values)

    def MapMessageRoutingValue(self, request, _context):
        return MapMessageRoutingValueResponse(routing_values=request.routing_values)


REQUEST_FIELD_CHUNKED = 'routing_values'
REQUEST_FIELD_SCALAR = 'routing_value'

SERVICE_1_ROUTING_VALUES = [42, 43]
SERVICE_2_ROUTING_VALUES = [9001, 9002]

# One routing value. Goes to service 1.
ONE_ROUTING_VALUE = [SERVICE_1_ROUTING_VALUES[0]]
# Two routing values. One for each service.
TWO_ROUTING_VALUES = [SERVICE_1_ROUTING_VALUES[0], SERVICE_2_ROUTING_VALUES[0]]
# Three routing values. Two go to service 1 and one goes to service 2.
THREE_ROUTING_VALUES = SERVICE_1_ROUTING_VALUES + [SERVICE_2_ROUTING_VALUES[0]]
# Four routing values. Two go to service 1 and two go to service 2.
FOUR_ROUTING_VALUES = SERVICE_1_ROUTING_VALUES + SERVICE_2_ROUTING_VALUES
# All known routing values.
ALL_ROUTING_VALUES = FOUR_ROUTING_VALUES


SERVICE_1_PORT = 50051
SERVICE_2_PORT = 50052

SERVICE_NAME = 'pigeon_test_service'
SERVICE_1 = Service(SERVICE_NAME, SERVICE_1_PORT, ports={'grpc': SERVICE_1_PORT})
SERVICE_2 = Service(SERVICE_NAME, SERVICE_2_PORT, ports={'grpc': SERVICE_2_PORT})


@pytest.mark.parametrize('routing_value', ALL_ROUTING_VALUES)
def test_scalar_request(routing_value, mocker):
    client = make_clients(mocker, routing_type=RoutingType.SCALAR, request_field=REQUEST_FIELD_SCALAR)
    rpc_callables = [client.ScalarRoutingValue]
    for rpc_callable in rpc_callables:
        request = ScalarRoutingValueRequest(routing_value=routing_value)
        response = rpc_callable(request)
        assert response.routing_value == routing_value

        request = ScalarRoutingValueRequest(routing_value=routing_value)
        future_response = rpc_callable.future(request).result()
        assert future_response.routing_value == routing_value


@pytest.mark.parametrize('routing_value', ALL_ROUTING_VALUES)
def test_round_robin_request(routing_value, mocker):
    client = make_clients(mocker, routing_type=RoutingType.ROUND_ROBIN, request_field=REQUEST_FIELD_SCALAR)
    rpc_callables = [client.ScalarRoutingValue]
    for rpc_callable in rpc_callables:
        request = ScalarRoutingValueRequest(routing_value=routing_value)
        response = rpc_callable(request)
        assert response.routing_value == routing_value

        request = ScalarRoutingValueRequest(routing_value=routing_value)
        future_response = rpc_callable.future(request).result()
        assert future_response.routing_value == routing_value


def test_list_no_values(mocker):
    client = make_clients(mocker)
    routing_values = []
    request = ListRoutingValueRequest(routing_values=routing_values)
    with pytest.raises(InvalidRoutingValueException):
        client.ListRoutingValue(request)

    request = ListRoutingValueRequest(routing_values=routing_values)
    with pytest.raises(InvalidRoutingValueException):
        client.ListRoutingValue.future(request)


@pytest.mark.parametrize(
    'routing_values', [ONE_ROUTING_VALUE, TWO_ROUTING_VALUES, THREE_ROUTING_VALUES, FOUR_ROUTING_VALUES]
)
def test_list_request(routing_values, mocker):
    client = make_clients(mocker)
    rpc_callables = [client.ListRoutingValue]
    for rpc_callable in rpc_callables:
        request = ListRoutingValueRequest(routing_values=routing_values)
        response = rpc_callable(request)
        assert set(response.routing_values) == set(routing_values)

        request = ListRoutingValueRequest(routing_values=routing_values)
        future_response = rpc_callable.future(request).result()
        assert set(future_response.routing_values) == set(routing_values)


@pytest.mark.parametrize('routing_value', ONE_ROUTING_VALUE)
def test_client_cleanup(routing_value, mocker):
    client = make_clients(mocker, routing_type=RoutingType.SCALAR, request_field=REQUEST_FIELD_SCALAR)
    for rpc_callable in [client.ScalarRoutingValue]:
        request = ScalarRoutingValueRequest(routing_value=routing_value)
        response = rpc_callable(request)
        assert response.routing_value == routing_value

        request = ScalarRoutingValueRequest(routing_value=routing_value)
        future_response = rpc_callable.future(request).result()
        assert future_response.routing_value == routing_value

    for key in set(client._clients).union(set(client._open_channels)):
        client._cleanup_client(key)


def test_map_no_values(mocker):
    client = make_clients(mocker)
    routing_values = {}
    request = MapRoutingValueRequest(routing_values=routing_values)
    with pytest.raises(InvalidRoutingValueException):
        client.MapRoutingValue(request)

    with pytest.raises(InvalidRoutingValueException):
        client.MapRoutingValue.future(request)


@pytest.mark.parametrize(
    'routing_values',
    [
        {ONE_ROUTING_VALUE[0]: 'foo'},
        {TWO_ROUTING_VALUES[0]: 'foo', TWO_ROUTING_VALUES[1]: 'bar'},
        {THREE_ROUTING_VALUES[0]: 'foo', THREE_ROUTING_VALUES[1]: 'bar', THREE_ROUTING_VALUES[2]: 'baz'},
        {
            FOUR_ROUTING_VALUES[0]: 'foo',
            FOUR_ROUTING_VALUES[1]: 'bar',
            FOUR_ROUTING_VALUES[2]: 'baz',
            FOUR_ROUTING_VALUES[3]: 'biz',
        },
    ],
)
def test_map_request(routing_values, mocker):
    client = make_clients(mocker)
    rpc_callables = [client.MapRoutingValue]
    for rpc_callable in rpc_callables:
        request = MapRoutingValueRequest(routing_values=routing_values)
        response = rpc_callable(request)
        assert response.routing_values == routing_values

        request = MapRoutingValueRequest(routing_values=routing_values)
        future_response = rpc_callable.future(request).result()
        assert future_response.routing_values == routing_values


FOO_1 = Foo(bar=1)
FOO_2 = Foo(bar=2)
FOO_3 = Foo(bar=3)
FOO_4 = Foo(bar=4)


@pytest.mark.parametrize(
    'routing_values',
    [
        {ONE_ROUTING_VALUE[0]: FOO_1},
        {TWO_ROUTING_VALUES[0]: FOO_1, TWO_ROUTING_VALUES[1]: FOO_2},
        {THREE_ROUTING_VALUES[0]: FOO_1, THREE_ROUTING_VALUES[1]: FOO_2, THREE_ROUTING_VALUES[2]: FOO_3},
        {
            FOUR_ROUTING_VALUES[0]: FOO_1,
            FOUR_ROUTING_VALUES[1]: FOO_2,
            FOUR_ROUTING_VALUES[2]: FOO_3,
            FOUR_ROUTING_VALUES[3]: FOO_4,
        },
    ],
)
def test_map_message_request(routing_values, mocker):
    client = make_clients(mocker)
    pb2_response_routing_values = {key: value for key, value in routing_values.items()}

    request = MapMessageRoutingValueRequest(routing_values=routing_values)
    pb2_response = client.MapMessageRoutingValue(request)
    assert pb2_response.routing_values == pb2_response_routing_values

    request = MapMessageRoutingValueRequest(routing_values=routing_values)
    future_pb2_response = client.MapMessageRoutingValue.future(request).result()
    assert future_pb2_response.routing_values == pb2_response_routing_values


def test_override_request_field(mocker):
    routing_value = SERVICE_1_ROUTING_VALUES[0]
    client = make_clients(mocker, request_field=REQUEST_FIELD_CHUNKED, routing_type=RoutingType.SCALAR)
    rpc_callables = [client.ScalarRoutingValue]
    for rpc_callable in rpc_callables:
        request = ScalarRoutingValueRequest(routing_value=routing_value)
        response = rpc_callable(request, request_field=REQUEST_FIELD_SCALAR)
        assert response.routing_value == routing_value

        request = ScalarRoutingValueRequest(routing_value=routing_value)
        future_response = rpc_callable.future(request, request_field=REQUEST_FIELD_SCALAR).result()
        assert future_response.routing_value == routing_value


def test_override_routing_type(mocker):
    routing_value = SERVICE_1_ROUTING_VALUES[0]
    client = make_clients(mocker, request_field=REQUEST_FIELD_SCALAR, routing_type=RoutingType.CHUNKED)
    rpc_callables = [client.ScalarRoutingValue]
    for rpc_callable in rpc_callables:
        request = ScalarRoutingValueRequest(routing_value=routing_value)
        response = rpc_callable(request, routing_type=RoutingType.SCALAR)
        assert response.routing_value == routing_value

        request = ScalarRoutingValueRequest(routing_value=routing_value)
        future_response = rpc_callable.future(request, routing_type=RoutingType.SCALAR).result()
        assert future_response.routing_value == routing_value


def make_clients(
    mocker, routing_type=RoutingType.CHUNKED, request_field=REQUEST_FIELD_CHUNKED
) -> RoutedClient[TestServiceStub]:
    def mock_select_service(_self, routing_value=None, secondaries=None, instances_to_skip=0):
        # Round robin.
        if routing_value is None:
            return SERVICE_1

        if routing_value not in ALL_ROUTING_VALUES:
            raise Exception(f'Unknown routing value. routing_value={routing_value} expected={ALL_ROUTING_VALUES}')

        if routing_value in SERVICE_1_ROUTING_VALUES:
            return SERVICE_1
        else:
            return SERVICE_2

    mocker.patch('osprey.worker.lib.discovery.service_watcher.ServiceWatcher.select', mock_select_service)
    # 1 so we trigger multiple calls to a service.
    chunk_size = 1
    client = RoutedClient(
        SERVICE_NAME,
        stub_cls=TestServiceStub,
        request_field=request_field,
        chunk_size=chunk_size,
        routing_type=routing_type,
        read_timeout=5.0,
    )
    return client


def run_server(port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    add_TestServiceServicer_to_server(TestService(), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    return server


@pytest.fixture(scope='session', autouse=True)
def run_servers(request):
    servers = [run_server(SERVICE_1_PORT), run_server(SERVICE_2_PORT)]

    def shutdown_servers():
        handlers = [server.stop(1) for server in servers]
        for handler in handlers:
            handler.wait()

    request.addfinalizer(shutdown_servers)
