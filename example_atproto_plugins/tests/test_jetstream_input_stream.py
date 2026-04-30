from atproto_plugin.jetstream_input_stream import _event_to_action

SAMPLE_POST_COMMIT = {
    'did': 'did:plc:abc123',
    'time_us': 1714500000000000,
    'kind': 'commit',
    'commit': {
        'rev': '3kf...',
        'operation': 'create',
        'collection': 'app.bsky.feed.post',
        'rkey': 'abcdefg',
        'cid': 'bafyrei...',
        'record': {
            '$type': 'app.bsky.feed.post',
            'text': 'this is a test post',
            'createdAt': '2024-04-30T12:00:00Z',
        },
    },
}


def test_post_commit_maps_to_operation_create():
    action = _event_to_action(SAMPLE_POST_COMMIT, action_id=42)

    assert action is not None
    assert action.action_id == 42
    assert action.action_name == 'operation#create'
    assert action.data['did'] == 'did:plc:abc123'
    op = action.data['operation']
    assert op['action'] == 'create'
    assert op['collection'] == 'app.bsky.feed.post'
    assert op['path'] == 'app.bsky.feed.post/abcdefg'
    assert op['cid'] == 'bafyrei...'
    assert op['record']['text'] == 'this is a test post'
    assert action.data['eventMetadata'] == {}


def test_like_commit_maps_to_operation_create_without_record_text():
    like = {
        **SAMPLE_POST_COMMIT,
        'commit': {
            **SAMPLE_POST_COMMIT['commit'],
            'collection': 'app.bsky.feed.like',
            'record': {'subject': {'uri': 'at://...', 'cid': 'bafy...'}},
        },
    }
    action = _event_to_action(like, action_id=1)

    assert action is not None
    assert action.action_name == 'operation#create'
    assert action.data['operation']['collection'] == 'app.bsky.feed.like'
    assert 'text' not in action.data['operation']['record']


def test_delete_commit_maps_with_empty_record():
    delete = {
        **SAMPLE_POST_COMMIT,
        'commit': {
            'operation': 'delete',
            'collection': 'app.bsky.feed.post',
            'rkey': 'abcdefg',
            'rev': '3kf...',
        },
    }
    action = _event_to_action(delete, action_id=2)

    assert action is not None
    assert action.action_name == 'operation#delete'
    assert action.data['operation']['action'] == 'delete'
    assert action.data['operation']['record'] == {}


def test_identity_event_maps_to_identity_action():
    identity = {
        'did': 'did:plc:xyz',
        'time_us': 1714500000000001,
        'kind': 'identity',
        'identity': {'did': 'did:plc:xyz', 'handle': 'someone.bsky.social', 'seq': 100},
    }
    action = _event_to_action(identity, action_id=3)

    assert action is not None
    assert action.action_name == 'identity'
    assert action.data['identity']['handle'] == 'someone.bsky.social'
    assert action.data['did'] == 'did:plc:xyz'


def test_account_event_is_skipped():
    account = {
        'did': 'did:plc:xyz',
        'time_us': 1714500000000002,
        'kind': 'account',
        'account': {'active': True, 'did': 'did:plc:xyz', 'seq': 100},
    }
    assert _event_to_action(account, action_id=4) is None


def test_event_without_time_us_is_skipped():
    bad = {k: v for k, v in SAMPLE_POST_COMMIT.items() if k != 'time_us'}
    assert _event_to_action(bad, action_id=5) is None


def test_unknown_kind_is_skipped():
    weird = {'did': 'did:plc:xyz', 'time_us': 1, 'kind': 'something-new'}
    assert _event_to_action(weird, action_id=6) is None
