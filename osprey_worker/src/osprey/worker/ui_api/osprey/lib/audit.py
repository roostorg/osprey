import json

from flask import Response, request
from osprey.worker.lib.snowflake import generate_snowflake
from osprey.worker.lib.storage.access_audit_log import AccessAuditLog
from werkzeug.datastructures import Headers

# To handle case insensitivity, ensure these are normalized to lower case.
_AUDIT_HEADERS_DISALLOW_LIST_LOWERCASE = {'cf-access-jwt-assertion', 'authorization', 'cf-access-jwt-payload', 'cookie'}

# Endpoints that are exempt from auditing:
_AUDIT_EXEMPT_ENDPOINTS = {'health', 'config.get_config'}


def _audit_serialize_headers(headers: Headers) -> str:
    headers_dict = {k: v for k, v in headers if k.lower() not in _AUDIT_HEADERS_DISALLOW_LIST_LOWERCASE}
    return json.dumps(headers_dict)


def audit_request(response: Response) -> Response:
    """Write a record of this request.

    NOTE: This will only be called for successful requests. If we want audit logs for requests that fail we can use
    Flask's `teardown_request` method as well.
    """
    if request.endpoint in _AUDIT_EXEMPT_ENDPOINTS:
        return response

    AccessAuditLog(
        id=generate_snowflake().to_int(),
        requester_email=getattr(request, 'claims', {}).get('email'),
        request_method=request.method,
        request_path=request.full_path,
        request_headers=_audit_serialize_headers(request.headers),
        request_body=request.data,
        response_status_code=response.status_code,
        response_headers=_audit_serialize_headers(response.headers),
        response_body=response.data,
    ).persist()

    return response
