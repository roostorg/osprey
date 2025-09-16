from abc import ABC
from functools import wraps
from http import HTTPStatus
from typing import Any, Callable, Type, TypeVar

from flask import Request, abort, jsonify, request
from pydantic import ValidationError
from sentry_sdk import capture_exception
from typing_extensions import Protocol

T = TypeVar('T', bound='Marshallable')


class Marshallable(Protocol):
    @classmethod
    def marshal(cls: Type[T], flask_request: Request) -> T: ...

    @classmethod
    def parse_obj(cls: Type[T], obj: Any) -> T: ...


class FlaskRequestMarshaller(ABC):
    """
    Mixin to provide marshalling capabilities with a pydantic `BaseModel`
    """

    @classmethod
    def marshal(cls: Type[T], flask_request: Request) -> T:
        raise NotImplementedError()


class JsonBodyMarshaller(FlaskRequestMarshaller):
    @classmethod
    def marshal(cls: Type[T], flask_request: Request) -> T:
        return cls.parse_obj(request.get_json(force=True))


class ArgMarshaller(FlaskRequestMarshaller):
    @classmethod
    def marshal(cls: Type[T], flask_request: Request) -> T:
        return cls.parse_obj(request.args)


class ViewArgMarshaller(FlaskRequestMarshaller):
    @classmethod
    def marshal(cls: Type[T], flask_request: Request) -> T:
        return cls.parse_obj(request.view_args)


class ViewArgAndJsonBodyMarshaller(FlaskRequestMarshaller):
    @classmethod
    def marshal(cls: Type[T], flask_request: Request) -> T:
        return cls.parse_obj({**request.view_args, **request.get_json(force=True)})


class ArgAndViewArgMarshaller(FlaskRequestMarshaller):
    @classmethod
    def marshal(cls: Type[T], flask_request: Request) -> T:
        return cls.parse_obj({**request.view_args, **request.args})


def marshal_with(model: Type[T]) -> Callable[[Callable[[T], Any]], Callable[[T], Any]]:
    """
    Decorator for flask view functions that marshals the incomming request data into a validated pydantic model

    The pydantic `model` must be mixed in with a `FlaskRequestMarshaller`
    """

    def decorator(f: Callable[[T], Any]) -> Callable[[T], Any]:
        @wraps(f)
        def decorated_function(*args: Any, **kwargs: Any) -> Any:
            try:
                model_instance = model.marshal(request)
            except ValidationError as err:
                response = jsonify(err.errors())
                capture_exception(err)
                response.status_code = HTTPStatus.BAD_REQUEST
                abort(response)

            return f(model_instance)

        return decorated_function

    return decorator
