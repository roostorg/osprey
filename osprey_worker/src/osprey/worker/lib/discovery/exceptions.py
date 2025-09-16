class ServiceException(Exception):
    """Base call for all discovery exceptions."""


class ServiceUnavailable(ServiceException):
    """Raised when there are no instances availiable for service."""
