import dataclasses
from typing import TYPE_CHECKING, Any, Callable, Mapping, Optional, cast

import simplejson
from flask import Flask
from pydantic import BaseModel

if TYPE_CHECKING:
    from _typeshed import DataclassInstance


class OspreyFlask(Flask):
    def make_response(self, rv: Any) -> Any:
        """
        Creates a response from a given view return value. Overrides the default flask behavior to be
        aware of pydantic types, and serialize them using simplejson, using the `bigint_as_string` option.
        """
        if not isinstance(rv, tuple):
            rv = (rv, {})

        # at this point, rv is now a tuple, the superclass's implementation expects
        # the first item of the tuple to be the "response". In this case however,
        # we are overriding the response handling here to try and coerce
        # more types into acceptable result types.
        response_object = rv[0]

        # Handle serializing pydantic models.
        if isinstance(response_object, BaseModel):
            rv = list(rv)
            rv[0] = self._json_encode_response(response_object.dict(), encoder=response_object.__json_encoder__)
            rv = tuple(rv)
        # Handle serializing dataclasses.
        elif dataclasses.is_dataclass(response_object):
            response_object = cast('DataclassInstance', response_object)
            rv = list(rv)
            rv[0] = self._json_encode_response(
                dataclasses.asdict(response_object),
                encoder=(
                    # Duck type check here. If we are dealing with a pydantic dataclass, we'll
                    # go ahead and use the provided json encoder, otherwise we'll use the default
                    # one.
                    response_object.__pydantic_model__.__json_encoder__
                    if hasattr(response_object, '__pydantic_model__')
                    else None
                ),
            )
            rv = tuple(rv)

        return super().make_response(rv)

    def _json_encode_response(self, rv: Mapping[str, object], encoder: Optional[Callable[[], Any]] = None) -> Any:
        return self.response_class(
            simplejson.dumps(
                rv,
                indent=2 if self.debug else None,
                sort_keys=self.debug,
                default=encoder,  # type: ignore
                bigint_as_string=True,
            ),
            mimetype='application/json',
        )
