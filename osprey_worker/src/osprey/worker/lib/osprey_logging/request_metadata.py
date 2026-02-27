from typing import Any

from flask import has_request_context, request
from osprey.worker.lib.osprey_logging.util import METADATA_FINALIZER_KEY, METADATA_KEY, USER_ID_KEY


def set_user_id(user_id):
    if not has_request_context():
        return

    request.environ[USER_ID_KEY] = str(user_id)


def set_user(user):
    set_user_id(user.id)


def get(key: str) -> Any | None:
    """
    Get a key from the request metadata.

    :type key: str
    :returns: The value of the key, or None if the key does not exist.
    """
    if not has_request_context() or METADATA_KEY not in request.environ:
        return None

    return request.environ[METADATA_KEY].get(key)


def update(**kwargs):
    """
    Updates the request metadata with the key-value pairs contained in kwargs to this function.
    This function is a no-op if not called within a request context.

    :param kwargs: dict
    """
    if not has_request_context():
        return

    if METADATA_KEY in request.environ:
        request.environ[METADATA_KEY].update(kwargs)
    else:
        request.environ[METADATA_KEY] = kwargs


def update_key(key, default, updater, finalizer=None):
    """
    This updates a key, setting it to `default` if it does not exist, then calling `updater` with the value previously
    set.

    This function is a no-op if not called within a request context. This means that the updater function will not
    be called if this function is called when not in a flask request context.

    For example, one could implement a simple counter by doing:

    request_metadata.update_key('query_count', 0, lambda i: i + 1)

    :type key: str
    :type default: any
    :type updater: callable
    :type finalizer: function to call with this value as the metadata is about to be serialized for logging.
    :returns: The value after being updated.
    """
    if not has_request_context():
        return

    if METADATA_KEY in request.environ:
        metadata = request.environ[METADATA_KEY]
    else:
        metadata = request.environ[METADATA_KEY] = {}

    if key not in metadata:
        value = default() if callable(default) else default
    else:
        value = metadata[key]

    new_value = updater(value)
    # If the updater returns None, chances are it just mutated the previous object provided
    # (i.e. lambda it: it.append(x)) - in which case we can just discard its value.
    if new_value is not None:
        value = new_value

    metadata[key] = value

    if finalizer is not None:
        _save_finalizer(key, finalizer)

    return value


def _save_finalizer(key, finalizer):
    if METADATA_FINALIZER_KEY not in request.environ:
        request.environ[METADATA_FINALIZER_KEY] = {}

    request.environ[METADATA_FINALIZER_KEY][key] = finalizer
