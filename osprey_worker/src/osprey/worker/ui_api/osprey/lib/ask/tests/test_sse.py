"""SSE serialization: versioned framing and serialization-error behavior."""

import json

import pytest
from osprey.worker.lib.ask import AskEvent
from osprey.worker.lib.ask.errors import SerializationError
from osprey.worker.ui_api.osprey.lib.ask.sse import format_sse


def test_frame_shape_and_versioned_payload():
    ev = AskEvent('assistant_message', {'text': 'hi'}, conversation_id='c1', turn_id='t1')
    frame = format_sse(ev)
    lines = frame.split('\n')
    assert lines[0] == 'event: assistant_message'
    assert lines[1].startswith('data: ')
    assert frame.endswith('\n\n')
    data = json.loads(lines[1][len('data: ') :])
    assert data['version'] == 1
    assert data['type'] == 'assistant_message'
    assert data['conversation_id'] == 'c1'
    assert data['turn_id'] == 't1'
    assert data['payload'] == {'text': 'hi'}


def test_non_serializable_payload_raises_serialization_error():
    ev = AskEvent('query_result', {'obj': object()})
    with pytest.raises(SerializationError):
        format_sse(ev)
