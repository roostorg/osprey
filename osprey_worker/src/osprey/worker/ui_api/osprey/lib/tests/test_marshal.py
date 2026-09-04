"""`lib/marshal`: what counts as a request body, and which source wins a collision.

Unit tests rather than endpoint tests. Every marshaller in this API reads its body
through `json_object_body`, so the rules are proved here once instead of being
re-proved against whichever endpoint happens to use them -- which is how four
hand-rolled `marshal()` implementations came to disagree with each other in the first
place. `views/tests` keeps only the thin checks that the wiring is in place: that a
refusal reaches the client as a 400 with no side effect behind it.

The app here is a bare `Flask`, not the osprey one: none of this needs a database, an
engine, or an authenticated user.
"""

from typing import Any

import pytest
from flask import Flask, jsonify, request
from osprey.worker.ui_api.osprey.lib.marshal import (
    JsonBodyMarshaller,
    RequestBodyNotAnObject,
    RequestBodyNotDeclaredJson,
    ViewArgAndOptionalJsonBodyMarshaller,
    json_object_body,
    marshal_with,
)
from pydantic import BaseModel
from werkzeug.exceptions import BadRequest

JSON = 'application/json'
FORM = 'application/x-www-form-urlencoded'

app = Flask(__name__)


class _BodyOnly(BaseModel, JsonBodyMarshaller):
    """No overrides: every field comes from the body."""

    name: str = 'unset'


class _AddressedByUrl(BaseModel, ViewArgAndOptionalJsonBodyMarshaller):
    """`thing_id` is in the path, so the body must not be able to choose it."""

    thing_id: int
    name: str = 'unset'


@app.route('/body', methods=['POST'])
@marshal_with(_BodyOnly)
def _body_view(model: _BodyOnly) -> object:
    return jsonify(model.dict())


@app.route('/thing/<int:thing_id>', methods=['POST'])
@marshal_with(_AddressedByUrl)
def _by_url_view(model: _AddressedByUrl) -> object:
    return jsonify(model.dict())


def _read_body(**request_kwargs: Any) -> dict:
    with app.test_request_context('/', **request_kwargs):
        return json_object_body(request)


# --------------------------------------------------------------------------- #
# json_object_body
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ('name', 'request_kwargs', 'expected'),
    [
        ('no body at all', {'method': 'POST'}, {}),
        ('declared JSON, empty body', {'method': 'POST', 'content_type': JSON, 'data': ''}, {}),
        ('declared JSON, whitespace body', {'method': 'POST', 'content_type': JSON, 'data': ' \n '}, {}),
        ('declared JSON, object body', {'method': 'POST', 'content_type': JSON, 'data': '{"a": 1}'}, {'a': 1}),
        ('empty JSON object', {'method': 'POST', 'content_type': JSON, 'data': '{}'}, {}),
        ('GET carrying a body', {'method': 'GET', 'content_type': JSON, 'data': '{"a": 1}'}, {}),
        ('HEAD carrying a body', {'method': 'HEAD', 'content_type': JSON, 'data': '{"a": 1}'}, {}),
        ('DELETE carrying a body', {'method': 'DELETE', 'content_type': JSON, 'data': '{"a": 1}'}, {}),
    ],
    ids=[
        'no-body',
        'declared-empty',
        'declared-whitespace',
        'object',
        'empty-object',
        'get-with-body',
        'head-with-body',
        'delete-with-body',
    ],
)
def test_json_object_body_reads_an_object_or_answers_nothing(name: str, request_kwargs: dict, expected: dict) -> None:
    """Sending nothing is allowed; what "nothing" means is decided here, not per model.

    An empty body is not a parse failure even though `json.loads('')` raises like one --
    a caller that set the content type out of habit and sent nothing is still a caller
    who sent nothing. Whether that was permitted is then the model's business: a field
    with a default is satisfied and a required one produces pydantic's own error.

    GET, HEAD and DELETE carry no body, so one sent anyway is not read at all rather
    than read and merged.
    """
    assert _read_body(**request_kwargs) == expected, name


@pytest.mark.parametrize(
    ('name', 'request_kwargs', 'raises'),
    [
        ('object body with no content type', {'data': '{"a": 1}'}, RequestBodyNotDeclaredJson),
        (
            'object body declared text/plain',
            {'content_type': 'text/plain', 'data': '{"a": 1}'},
            RequestBodyNotDeclaredJson,
        ),
        ('form-encoded body', {'content_type': FORM, 'data': 'a=1'}, RequestBodyNotDeclaredJson),
        ('declared JSON that does not parse', {'content_type': JSON, 'data': '{"a": tru'}, BadRequest),
        ('JSON array', {'content_type': JSON, 'data': '[1, 2]'}, RequestBodyNotAnObject),
        ('JSON string', {'content_type': JSON, 'data': '"a"'}, RequestBodyNotAnObject),
        ('JSON number', {'content_type': JSON, 'data': '1'}, RequestBodyNotAnObject),
        ('JSON null', {'content_type': JSON, 'data': 'null'}, RequestBodyNotAnObject),
    ],
    ids=['undeclared', 'text-plain', 'form', 'unparseable', 'array', 'string', 'number', 'null'],
)
def test_json_object_body_refuses_a_body_it_cannot_use(
    name: str, request_kwargs: dict, raises: type[Exception]
) -> None:
    """Bytes arrived and could not be applied, so the request is refused, not defaulted.

    Each of these used to end as a silent success somewhere in this app: `get_json`
    answers `None` for the first three, which read as "no body" and dropped what the
    caller sent, and the last four parsed into something that could not be spread into a
    model -- which `EntityMarshaller` did anyway, raising `TypeError` for a 500.

    The unparseable case raises werkzeug's `BadRequest` rather than
    `UnreadableRequestBody`, because it comes from `get_json` itself. Both end as a 400,
    but by different routes and with different response bodies; unifying that means
    overriding `Request.on_json_loading_failed` app-wide.
    """
    with pytest.raises(raises):
        _read_body(method='POST', **request_kwargs)


# --------------------------------------------------------------------------- #
# The marshaller family
# --------------------------------------------------------------------------- #


def test_a_marshaller_with_no_overrides_takes_everything_from_the_body() -> None:
    with app.test_client() as client:
        res = client.post('/body', json={'name': 'from-body'})

    assert res.status_code == 200, res.data
    assert res.json == {'name': 'from-body'}


def test_a_marshaller_falls_back_to_its_defaults_when_no_body_is_sent() -> None:
    """The optional-body case: nothing sent, so every field keeps its default."""
    with app.test_client() as client:
        res = client.post('/body')

    assert res.status_code == 200, res.data
    assert res.json == {'name': 'unset'}


def test_overrides_win_a_collision_with_the_body() -> None:
    """The URL selects the row; a body key of the same name must not be able to move it.

    `overrides` is merged last for exactly this reason. With the body last, a request to
    `/thing/1` carrying `{"thing_id": 2}` would act on 2 while the URL, the logs and any
    audit record all said 1.
    """
    with app.test_client() as client:
        res = client.post('/thing/1', json={'thing_id': 2, 'name': 'from-body'})

    assert res.status_code == 200, res.data
    assert res.json == {'thing_id': 1, 'name': 'from-body'}


def test_overrides_still_apply_when_no_body_is_sent() -> None:
    with app.test_client() as client:
        res = client.post('/thing/7')

    assert res.status_code == 200, res.data
    assert res.json == {'thing_id': 7, 'name': 'unset'}


@pytest.mark.parametrize(
    ('name', 'post_kwargs'),
    [
        ('a body that is not an object', {'data': '[1, 2]', 'content_type': JSON}),
        ('a body that does not parse', {'data': '{"a": tru', 'content_type': JSON}),
        ('a body with no content type', {'data': '{"name": "x"}'}),
    ],
    ids=['array', 'unparseable', 'undeclared'],
)
def test_marshal_with_turns_an_unusable_body_into_a_400(name: str, post_kwargs: dict) -> None:
    """The refusal reaches the client as a 400 rather than escaping as a 500.

    `UnreadableRequestBody` is not an `HTTPException`, so this pins that `marshal_with`
    catches it -- without that, every refusal above would be an unhandled exception.
    """
    with app.test_client() as client:
        res = client.post('/body', **post_kwargs)

    assert res.status_code == 400, (name, res.data)


def test_the_error_a_client_is_shown_is_a_constant_not_the_request() -> None:
    """The 400 body is the exception's class attribute, so it can't reflect the request.

    `str(err)` here would make the response text whatever a raise site passed in, and the
    tempting thing to pass in is something read off the request. That is the shape CodeQL
    flagged as code-scanning/26 elsewhere in this API, so this pins that the message a
    client sees is fixed text and that nothing sent in the request comes back out.
    """
    with app.test_client() as client:
        res = client.post('/body', data='{"secret": "hunter2"}', content_type='text/plain')

    assert res.status_code == 400
    assert res.json == {'error': RequestBodyNotDeclaredJson.message}
    assert b'hunter2' not in res.data
    assert b'text/plain' not in res.data
