from abc import ABC
from collections.abc import Callable
from functools import wraps
from http import HTTPStatus
from typing import Any, Type, TypeVar

from flask import Request, abort, jsonify, request
from pydantic import ValidationError
from sentry_sdk import capture_exception
from typing_extensions import Protocol

T = TypeVar('T', bound='Marshallable')


class UnreadableRequestBody(Exception):
    """The request didn't arrive as JSON this API can read.

    Raised by `json_object_body`, mapped to a 400 by `marshal_with`, so a marshaller
    stays out of the business of building responses.

    `message` is what the client is told, and it is a class attribute rather than a
    constructor argument so that every string a caller can see is written here. Sending
    `str(err)` instead would put the text under the control of whichever raise site ran,
    and the natural thing to interpolate at a raise site is something off the request --
    which is how an error message becomes a way to read server state back out. The same
    reasoning keeps the configured path off `RulesDirNotADirectory`'s message in
    `lib/rule_deployment`, where CodeQL flagged it as code-scanning/26.

    Subclassed per failure so the client learns which of the two it hit without any of
    them being assembled from the request.
    """

    message = 'Request body could not be read as JSON.'


class RequestBodyNotDeclaredJson(UnreadableRequestBody):
    message = 'A request body must be sent with Content-Type: application/json.'


class RequestBodyNotAnObject(UnreadableRequestBody):
    message = 'Request body must be a JSON object.'


# The methods that carry no body, so nothing sent on one is read. Written as the
# bodyless set rather than the submitting one so the default is to check: a method not
# named here is treated as able to carry a body, and anything it sends has to hold up.
_BODYLESS_METHODS = frozenset({'GET', 'HEAD', 'DELETE'})


def json_object_body(flask_request: Request) -> dict[str, Any]:
    """The request's JSON object body, or `{}` if it carried none.

    Every marshaller that reads a body reads it through here, because "what counts as a
    body" is the same question every time and it is easy to answer differently by
    accident. It used to be answered separately in four places, which between them
    dropped unlabelled bodies on the floor, returned 200 for bodies that never parsed,
    and raised `TypeError` -- a 500 -- on a body that was a JSON list.

    The difficulty is that `get_json` answers `None` to three different questions: a
    body that wasn't labelled `application/json`, a body that didn't parse, and no body
    at all. Reading `None` as "the caller sent nothing" collapses the first two into the
    third, so a request whose body was dropped is answered as though it had asked for
    the defaults -- and the endpoint reports success for work it did not do.

    So the question asked here is whether bytes arrived, not which method carried them.
    If they did, they must be declared JSON and must parse to an object. If they didn't,
    the model's own fields decide whether that was allowed: a field with a default is
    satisfied, and a required one produces pydantic's own error without this function
    having an opinion about it.

    GET, HEAD and DELETE carry no body, so anything sent on one is not read at all.
    """
    if flask_request.method in _BODYLESS_METHODS or not flask_request.get_data().strip():
        return {}

    if not flask_request.is_json:
        raise RequestBodyNotDeclaredJson()

    body = flask_request.get_json()
    if not isinstance(body, dict):
        raise RequestBodyNotAnObject()

    return body


class Marshallable(Protocol):
    """What `marshal_with` needs of a request model: something that can read a request.

    The methods below are bodied by their docstrings rather than by `...`. Both are
    valid, and neither ever runs -- a `Protocol` is structural, so nothing dispatches to
    these -- but CodeQL reads a bare `...` as a statement with no effect, and a docstring
    says what the method is for while it satisfies the checker. Not `raise
    NotImplementedError`, which would imply a runtime dispatch that cannot happen here.
    """

    @classmethod
    def marshal(cls: Type[T], flask_request: Request) -> T:
        """Build the model from `flask_request`, however this model sources its fields."""

    @classmethod
    def parse_obj(cls: Type[T], obj: Any) -> T:
        """Validate a mapping into the model. Supplied by pydantic's `BaseModel`."""


class MarshallableWithOverrides(Marshallable, Protocol):
    """A `Marshallable` that also says which of its fields don't come from the body.

    Separate from `Marshallable` so that arg-only marshallers -- which never read a body
    and so have nothing to override it with -- aren't made to declare a hook they don't
    use. `marshal_with` only ever needs `Marshallable`.
    """

    @classmethod
    def overrides(cls, flask_request: Request) -> dict[str, Any]:
        """Fields read from somewhere other than the body -- the URL, usually."""


TOverriding = TypeVar('TOverriding', bound='MarshallableWithOverrides')


class FlaskRequestMarshaller(ABC):
    """
    Mixin to provide marshalling capabilities with a pydantic `BaseModel`
    """

    @classmethod
    def marshal(cls: Type[T], flask_request: Request) -> T:
        raise NotImplementedError()


class JsonBodyMarshaller(FlaskRequestMarshaller):
    """A JSON object body, with whatever `overrides` reads elsewhere merged over it.

    Subclasses supply `overrides` rather than a `marshal` of their own, so reading the
    body is not a question anyone has to answer twice: a marshaller written against this
    base never touches the body API, and so cannot get it wrong. What varies between
    marshallers is only where their *other* fields come from.

    `overrides` wins collisions. What it reads comes from the URL, which is what a
    reviewer reads and what the audit log records, so a body key must not be able to
    disagree with it.
    """

    @classmethod
    def overrides(cls, flask_request: Request) -> dict[str, Any]:
        """Fields read from somewhere other than the body -- the URL, usually."""
        return {}

    @classmethod
    def marshal(cls: Type[TOverriding], flask_request: Request) -> TOverriding:
        return cls.parse_obj({**json_object_body(flask_request), **cls.overrides(flask_request)})


class ArgMarshaller(FlaskRequestMarshaller):
    @classmethod
    def marshal(cls: Type[T], flask_request: Request) -> T:
        return cls.parse_obj(request.args)


class ViewArgMarshaller(FlaskRequestMarshaller):
    @classmethod
    def marshal(cls: Type[T], flask_request: Request) -> T:
        return cls.parse_obj(request.view_args)


class ViewArgAndOptionalJsonBodyMarshaller(JsonBodyMarshaller):
    """A JSON object body addressed by URL: the view args win any collision."""

    @classmethod
    def overrides(cls, flask_request: Request) -> dict[str, Any]:
        return flask_request.view_args or {}


class ArgAndViewArgMarshaller(FlaskRequestMarshaller):
    @classmethod
    def marshal(cls: Type[T], flask_request: Request) -> T:
        return cls.parse_obj({**request.view_args, **request.args})


def marshal_with(model: Type[T]) -> Callable[[Callable[[T], Any]], Callable[[T], Any]]:
    """
    Decorator for flask view functions that marshals the incoming request data into a validated pydantic model

    The pydantic `model` must be mixed in with a `FlaskRequestMarshaller`
    """

    def decorator(f: Callable[[T], Any]) -> Callable[[T], Any]:
        @wraps(f)
        def decorated_function(*args: Any, **kwargs: Any) -> Any:
            try:
                model_instance = model.marshal(request)
            except UnreadableRequestBody as err:
                # `err.message`, not `str(err)`: the text sent back is a class attribute
                # on the exception, so nothing read off the request can be reflected into
                # a response body. See `UnreadableRequestBody`.
                #
                # Not reported to Sentry, unlike the validation errors below: a request
                # that isn't JSON never reached the model, so there is no field-level
                # detail to capture, and it says nothing about this service.
                response = jsonify({'error': err.message})
                response.status_code = HTTPStatus.BAD_REQUEST
                abort(response)
            except ValidationError as err:
                response = jsonify(err.errors())
                capture_exception(err)
                response.status_code = HTTPStatus.BAD_REQUEST
                abort(response)

            return f(model_instance)

        return decorated_function

    return decorator
