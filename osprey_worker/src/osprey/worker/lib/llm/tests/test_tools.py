"""Unit tests for the vendor-neutral tool declaration/registry/dispatch layer."""

from osprey.worker.lib.llm import (
    Tool,
    ToolCall,
    ToolDefinition,
    ToolParameter,
    ToolRegistry,
    build_input_schema,
)


def test_build_input_schema_required_optional_enum_default() -> None:
    schema = build_input_schema(
        [
            ToolParameter(name='query', type='string', description='the query'),
            ToolParameter(name='limit', type='integer', description='max results', required=False, default=5),
            ToolParameter(
                name='order',
                type='string',
                description='sort order',
                required=False,
                enum=['asc', 'desc'],
            ),
        ]
    )

    assert schema == {
        'type': 'object',
        'properties': {
            'query': {'type': 'string', 'description': 'the query'},
            'limit': {'type': 'integer', 'description': 'max results', 'default': 5},
            'order': {'type': 'string', 'description': 'sort order', 'enum': ['asc', 'desc']},
        },
        'required': ['query'],
    }


def test_build_input_schema_omits_required_when_none() -> None:
    schema = build_input_schema([ToolParameter(name='x', type='number', description='x', required=False)])
    assert 'required' not in schema


def test_decorator_registers_and_returns_callable() -> None:
    registry = ToolRegistry()

    @registry.tool(
        name='lookup_user',
        description='Look up a user by id.',
        parameters=[ToolParameter(name='id', type='integer', description='user id')],
    )
    def lookup_user(id: int) -> dict:
        return {'id': id, 'name': 'Ada'}

    # The wrapped function is returned unchanged and stays directly callable.
    assert lookup_user(7) == {'id': 7, 'name': 'Ada'}

    tool = registry.get('lookup_user')
    assert isinstance(tool, Tool)
    assert [t.name for t in registry.all_tools()] == ['lookup_user']


def test_definitions_produce_tool_definitions() -> None:
    registry = ToolRegistry()

    @registry.tool(
        name='ping', description='ping', parameters=[ToolParameter(name='msg', type='string', description='m')]
    )
    def ping(msg: str) -> str:
        return msg

    definitions = registry.definitions()
    assert len(definitions) == 1
    definition = definitions[0]
    assert isinstance(definition, ToolDefinition)
    assert definition.name == 'ping'
    assert definition.input_schema == {
        'type': 'object',
        'properties': {'msg': {'type': 'string', 'description': 'm'}},
        'required': ['msg'],
    }


def test_dispatch_success_serializes_non_string_result() -> None:
    registry = ToolRegistry()

    @registry.tool(name='add', description='add', parameters=[])
    def add(a: int, b: int) -> dict:
        return {'sum': a + b}

    result = registry.dispatch(ToolCall(id='c1', name='add', arguments={'a': 2, 'b': 3}))
    assert result.tool_call_id == 'c1'
    assert result.is_error is False
    assert result.content == '{"sum": 5}'


def test_dispatch_passes_through_string_result() -> None:
    registry = ToolRegistry()

    @registry.tool(name='echo', description='echo', parameters=[])
    def echo(text: str) -> str:
        return text

    result = registry.dispatch(ToolCall(id='c1', name='echo', arguments={'text': 'hi'}))
    assert result.content == 'hi'
    assert result.is_error is False


def test_dispatch_handler_exception_is_error() -> None:
    registry = ToolRegistry()

    @registry.tool(name='boom', description='boom', parameters=[])
    def boom() -> None:
        raise ValueError('kaboom')

    result = registry.dispatch(ToolCall(id='c1', name='boom', arguments={}))
    assert result.is_error is True
    assert 'ValueError' in result.content
    assert 'kaboom' in result.content


def test_dispatch_unknown_tool_is_error() -> None:
    registry = ToolRegistry()
    result = registry.dispatch(ToolCall(id='c1', name='nope', arguments={}))
    assert result.is_error is True
    assert 'nope' in result.content


def test_dispatch_unserializable_result_is_error() -> None:
    registry = ToolRegistry()

    @registry.tool(name='cyclic', description='returns a circular reference', parameters=[])
    def cyclic() -> dict:
        d: dict = {}
        d['self'] = d
        return d

    # A non-serialisable return is captured as an error rather than propagating.
    result = registry.dispatch(ToolCall(id='c1', name='cyclic', arguments={}))
    assert result.is_error is True


def test_duplicate_registration_raises() -> None:
    registry = ToolRegistry()

    @registry.tool(name='dup', description='d', parameters=[])
    def first() -> None:
        return None

    try:
        registry.tool(name='dup', description='d', parameters=[])(first)
    except ValueError:
        pass
    else:
        raise AssertionError('expected duplicate registration to raise')
