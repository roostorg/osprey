from typing import Optional, cast

import grpc


class PigeonException(Exception):
    pass


class InvalidRoutingValueException(PigeonException):
    def __init__(self, field: str, value):
        self.field = field
        self.value = value

    def __str__(self):
        return f'Field {self.field} is empty. value={self.value}'


class NoResponsesException(PigeonException):
    pass


class RPCException(PigeonException, grpc.RpcError, grpc.Call):  # type: ignore[misc]
    # https://grpc.github.io/grpc/python/grpc.html#grpc.Call
    def __init__(self, service: str, method: str, error: grpc.RpcError):
        # grpc.RpcError is always a grpc.Call
        inner = cast(grpc.Call, error)
        message = f'{service}.{method} failed. status={inner.code()} details="{inner.details()}" inner={inner}'
        super().__init__(message)
        self._service = service
        self._method = method
        self._inner = inner

    @property
    def service(self) -> str:
        return self._service

    @property
    def method(self) -> str:
        return self._method

    @property
    def inner(self) -> grpc.Call:
        return self._inner

    def initial_metadata(self):
        return self.inner.initial_metadata()

    def trailing_metadata(self):
        return self.inner.trailing_metadata()

    def code(self) -> grpc.StatusCode:
        return cast(grpc.StatusCode, self.inner.code())

    def details(self) -> str:
        return cast(str, self.inner.details())

    def is_active(self) -> bool:
        return cast(bool, self.inner.is_active())

    def time_remaining(self) -> Optional[float]:
        return self.inner.time_remaining()

    def cancel(self):
        return self.inner.cancel()

    def add_callback(self, callback):
        return self.inner.add_callback(callback)
