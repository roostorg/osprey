from osprey.rpc.pigeon.tests.v1 import tests_pb2
from osprey.worker.lib.pigeon.client import _make_message

REQUEST_FIELD = 'routing_values'


def test_make_message_list():
    message_template = tests_pb2.ListRoutingValueRequest()
    routing_values = [43, 56, 10, 54]
    routing_values_chunk = routing_values[0:2]
    message = _make_message(message_template, REQUEST_FIELD, routing_values, routing_values_chunk)
    assert message.routing_values == routing_values_chunk


def test_make_message_list_empty():
    message_template = tests_pb2.ListRoutingValueRequest()
    routing_values = []
    routing_values_chunk = routing_values
    message = _make_message(message_template, REQUEST_FIELD, routing_values, routing_values_chunk)
    assert message.routing_values == routing_values_chunk


def test_make_message_map():
    message_template = tests_pb2.MapRoutingValueRequest()
    routing_values = {43: 'foo', 56: 'bar', 10: 'baz', 54: 'boo'}
    routing_values_chunk = [43, 56]
    message = _make_message(message_template, REQUEST_FIELD, routing_values, routing_values_chunk)
    assert message.routing_values == {43: 'foo', 56: 'bar'}


def test_make_message_map_empty():
    message_template = tests_pb2.MapRoutingValueRequest()
    routing_values = {}
    routing_values_chunk = []
    message = _make_message(message_template, REQUEST_FIELD, routing_values, routing_values_chunk)
    assert message.routing_values == routing_values


def test_make_message_map_message():
    message_template = tests_pb2.MapMessageRoutingValueRequest()
    foo_1 = tests_pb2.Foo(bar=1)
    foo_2 = tests_pb2.Foo(bar=2)
    routing_values = {42: foo_1, 56: foo_2, 10: tests_pb2.Foo(), 54: tests_pb2.Foo()}
    routing_values_chunk = [42, 56]
    message = _make_message(message_template, REQUEST_FIELD, routing_values, routing_values_chunk)
    assert message.routing_values == {42: foo_1, 56: foo_2}


def test_make_message_map_message_empty():
    message_template = tests_pb2.MapMessageRoutingValueRequest()
    routing_values = {}
    routing_values_chunk = []
    message = _make_message(message_template, REQUEST_FIELD, routing_values, routing_values_chunk)
    assert message.routing_values == routing_values
