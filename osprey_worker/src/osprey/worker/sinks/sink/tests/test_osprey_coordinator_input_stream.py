from typing import Optional
from unittest.mock import patch

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from osprey.rpc.osprey_coordinator.bidirectional_stream.v1.service_pb2 import OspreyCoordinatorAction
from osprey.worker.sinks.sink.osprey_coordinator_input_stream import OspreyCoordinatorInputStream


def create_coordinator_action(secret_data: Optional[bytes]) -> OspreyCoordinatorAction:
    action = OspreyCoordinatorAction(
        action_id=123,
        action_name='test_action',
        json_action_data=b'{"public":"value"}',
        timestamp=Timestamp(seconds=1_700_000_000),
    )
    if secret_data is not None:
        action.json_secret_data = secret_data
    return action


@pytest.mark.parametrize(
    ('encoded_secret_data', 'expected_secret_data'),
    [
        (None, {}),
        (b'{"private":"secret"}', {'private': 'secret'}),
    ],
)
def test_create_engine_action_keeps_secret_data_separate(
    encoded_secret_data: Optional[bytes], expected_secret_data: dict[str, str]
) -> None:
    stream = OspreyCoordinatorInputStream.__new__(OspreyCoordinatorInputStream)

    coordinator_action = create_coordinator_action(encoded_secret_data)

    action = stream._create_osprey_engine_action(coordinator_action)

    assert action is not None
    assert action.data == {'public': 'value'}
    assert action.secret_data == expected_secret_data
    assert not coordinator_action.HasField('json_secret_data')


def test_create_engine_action_rejects_malformed_secret_json_without_capturing_plaintext() -> None:
    stream = OspreyCoordinatorInputStream.__new__(OspreyCoordinatorInputStream)
    coordinator_action = create_coordinator_action(b'private-secret-sentinel')

    with (
        patch(
            'osprey.worker.sinks.sink.osprey_coordinator_input_stream.sentry_sdk.capture_exception'
        ) as capture_exception,
        patch('osprey.worker.sinks.sink.osprey_coordinator_input_stream.logger.warning') as warning,
    ):
        action = stream._create_osprey_engine_action(coordinator_action)

    assert action is None
    assert not coordinator_action.HasField('json_secret_data')
    capture_exception.assert_not_called()
    warning.assert_called_once_with(
        'Error while generating input message containing secret data: %s',
        'JSONDecodeError',
    )
