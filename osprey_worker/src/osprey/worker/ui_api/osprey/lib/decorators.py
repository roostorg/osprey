import functools

from flask import abort, request


def require_multipart_form_data(func):
    """
    Decorator that validates the request has multipart/form-data content type.

    This decorator ensures that POST, PUT, and PATCH requests use the correct
    content type for file uploads. Returns HTTP 400 if the content type is invalid.

    Usage:
        @require_multipart_form_data
        def upload_file(job_id, file_name):
            file = request.files.get('file')
            # ... handle file upload
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if request.method in {'PUT', 'PATCH', 'POST'} and request.mimetype != 'multipart/form-data':
            abort(400, description='Content-Type must be multipart/form-data')
        return func(*args, **kwargs)

    return wrapper
