import json
from typing import cast
from unittest.mock import MagicMock, patch

import msgpack
import pytest
from _pytest.fixtures import FixtureRequest
from google.pubsub_v1 import PubsubMessage
from osprey.engine.executor.execution_context import Action
from osprey.worker.lib.encryption.envelope import Envelope
from osprey.worker.sinks.sink.input_stream import AsyncPubSubOspreyActionInputStream


@pytest.fixture(params=[True, False])
def with_encryption(request: FixtureRequest) -> bool:
    return cast(bool, request.param)


@pytest.fixture
def osprey_action() -> Action:
    return Action(
        action_name='auth_session_created',
        action_id=4321,
        data={
            'user': {'id': '1234', 'bot': False, 'mfa_enabled': False, 'flags': '0'},
            'user_session': {'id_hash': 'Ynl0ZV9oYXNo', 'session_version': 'VERSION_LEGACY_UNSPECIFIED'},
        },
        timestamp=None,
    )


@pytest.fixture
def osprey_action_msgpack(osprey_action: Action, with_encryption: bool) -> bytes:
    # need to drop action_* from the keys for the json format
    action_dict = {
        'id': osprey_action.action_id,
        'name': osprey_action.action_name,
        'data': osprey_action.data,
        'timestamp': osprey_action.timestamp,
    }
    if with_encryption:
        action_dict['secret_data'] = osprey_action.secret_data
    return msgpack.dumps(json.dumps(action_dict))


def test_json_action(pubsub_subscriber_mock: MagicMock, osprey_action: Action, osprey_action_msgpack: bytes):
    pubsub_input_stream = AsyncPubSubOspreyActionInputStream(
        subscriber=pubsub_subscriber_mock, subscription_path='what/ever'
    )
    pubsub_message = PubsubMessage(data=osprey_action_msgpack, attributes={'encoding': 'msgpack'})
    assert osprey_action == pubsub_input_stream._create_action(pubsub_message)


def test_secret_decryption(pubsub_subscriber_mock: MagicMock, osprey_action_msgpack: bytes, with_encryption: bool):
    with patch('osprey.worker.sinks.sink.input_stream.Envelope') as mock:
        envelope_mock = MagicMock(spec=Envelope)
        envelope_mock.decrypt = MagicMock(return_value=osprey_action_msgpack)
        mock.return_value = envelope_mock
        pubsub_input_stream = AsyncPubSubOspreyActionInputStream(
            subscriber=pubsub_subscriber_mock, subscription_path='what/ever', kek_uri='gcp-kms://fake_uri'
        )
        attributes = {'encoding': 'msgpack', 'encrypted': str(with_encryption).lower()}
        pubsub_message = PubsubMessage(data=osprey_action_msgpack, attributes=attributes)
        pubsub_input_stream._create_action(pubsub_message)
        assert envelope_mock.decrypt.called == with_encryption
