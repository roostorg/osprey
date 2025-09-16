import json

import pytest
from flask import Flask, has_request_context, request
from osprey.worker.lib.osprey_logging import request_metadata
from osprey.worker.lib.osprey_logging.util import METADATA_KEY, USER_ID_KEY, get_metadata


class DummyUser:
    def __init__(self, user_id):
        self.id = user_id


@pytest.fixture()
def app():
    app = Flask(__name__)
    return app


def test_set_user(app):
    with app.test_request_context('/'):
        request_metadata.set_user(DummyUser(500))
        assert request.environ[USER_ID_KEY] == '500'


def test_set_user_id(app):
    with app.test_request_context('/'):
        request_metadata.set_user_id(501)
        assert request.environ[USER_ID_KEY] == '501'


def test_set_user_id_fails_silently_if_no_request_context():
    assert not has_request_context()
    request_metadata.set_user_id(502)
    assert True


def test_update(app):
    with app.test_request_context('/'):
        request_metadata.update(foo='bar', baz='qux')
        assert request.environ[METADATA_KEY] == {'foo': 'bar', 'baz': 'qux'}

        request_metadata.update(baz='bab', boo='bop')
        assert request.environ[METADATA_KEY] == {'foo': 'bar', 'baz': 'bab', 'boo': 'bop'}


def test_update_key(app):
    def incr(it):
        return it + 1

    with app.test_request_context('/'):
        assert request_metadata.update_key('counter', 0, incr) == 1
        assert request_metadata.update_key('counter', 0, incr) == 2
        assert request_metadata.update_key('counter', 0, incr) == 3

        assert request.environ[METADATA_KEY] == {'counter': 3}


def test_update_key_supports_callable(app):
    with app.test_request_context('/'):
        assert request_metadata.update_key('list', list, lambda it: it.append(1)) == [1]
        assert request_metadata.update_key('list', list, lambda it: it.append(2)) == [1, 2]
        assert request_metadata.update_key('list', list, lambda it: it.append(3)) == [1, 2, 3]

        assert request.environ[METADATA_KEY] == {'list': [1, 2, 3]}


def test_update_key_fails_silently_with_no_request_context():
    assert not has_request_context()
    assert request_metadata.update_key('counter', 0, lambda it: it) is None


def test_update_fails_silently_with_no_request_context():
    assert not has_request_context()
    request_metadata.update(foo='bar')
    assert True


def test_request_metadata_finalizer(app):
    with app.test_request_context('/'):
        assert request_metadata.update_key('list', list, lambda it: it.append(1), finalizer=json.dumps) == [1]
        assert request_metadata.update_key('list', list, lambda it: it.append(2), finalizer=json.dumps) == [1, 2]
        assert request_metadata.update_key('list', list, lambda it: it.append(3), finalizer=json.dumps) == [1, 2, 3]

        assert request.environ[METADATA_KEY] == {'list': [1, 2, 3]}

        assert get_metadata(request.environ) == {'list': '[1, 2, 3]'}


def test_request_metadata_finalizer_with_exception(app):
    def just_throws(*args):
        raise ValueError('whoops!')

    with app.test_request_context('/'):
        assert request_metadata.update_key('list', list, lambda it: it.append(1), finalizer=just_throws) == [1]
        request_metadata.update(a='b')

        assert get_metadata(request.environ) == {'a': 'b'}
