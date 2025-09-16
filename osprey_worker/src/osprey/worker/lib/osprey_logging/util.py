import os
import time

METADATA_KEY = 'METADATA'
USER_ID_KEY = 'USER'
METADATA_FINALIZER_KEY = 'METADATA_FINALIZERS'


# NOTE: this function sits here because its imported by gunicorn on initialization
#  before we've had gevent monkey patch things, so it must sit in a module that
#  does not import anything that can conflict with gevent monkey patching.
def environ_to_logging_dict(environ, status_code, content_length, request_time):
    """
    This function takes some common request variables and returns a dictionary
    that can be jsonified and used as logging output.
    """
    return {
        'http_remote_addr': environ.get('REMOTE_ADDR'),
        'http_method': environ.get('REQUEST_METHOD'),
        'http_route': environ['RAW_URI'],
        'http_version': environ['SERVER_PROTOCOL'],
        'http_status_code': status_code,
        'http_content_length': content_length,
        'http_referrer': environ.get('HTTP_REFERER'),
        'http_user_agent': environ.get('HTTP_USER_AGENT'),
        'http_response_time': request_time,
        'http_cf_ray': environ.get('HTTP_CF_RAY'),
        'http_debug_options': environ.get('HTTP_X_DEBUG_OPTIONS'),
        'api_version': environ.get('API_VERSION') if environ.get('REQUEST_HAS_DECLARED_API_VERSION') else None,
        'endpoint': environ.get('ENDPOINT'),
        'timestamp': time.time(),
        'user_id': environ.get(USER_ID_KEY),
        'pid': os.getpid(),
        'metadata': get_metadata(environ),
    }


def get_metadata(environ):
    """
    Returns the request metadata, applying any finalizers before returning the dictionary of request metadata
    :type environ: dict
    :rtype: dict
    """
    finalizers = environ.get(METADATA_FINALIZER_KEY, None)
    metadata = environ.get(METADATA_KEY) or {}

    if finalizers:
        # Finalizers should always be the smaller set.
        for key, finalizer_fn in finalizers.items():
            if key in metadata:
                try:
                    metadata[key] = finalizer_fn(metadata[key])
                except Exception:
                    del metadata[key]

    return metadata
