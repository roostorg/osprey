from contextlib import contextmanager
from typing import Callable, Generic, Iterator, Optional, TypeVar

T = TypeVar('T')


class Singleton(Generic[T]):
    """A generic singleton.

    Example usage:
    ```
        class Foo:
            def bar(self):
                return "hi"

        foo_singleton = Singleton(Foo)
        foo_singleton.instance().bar()
        assert foo_singleton.instance() is foo_singleton.instance()
    ```
    """

    def __init__(self, factory: Callable[[], T]):
        self._factory = factory
        self._instance: Optional[T] = None

    def instance(self) -> T:
        """Gets or lazily creates the singleton object by invoking the factory function."""
        if self._instance is None:
            self._instance = self._factory()

        return self._instance

    def set_instance(self, instance: T) -> 'Singleton[T]':
        if self._instance is not None:
            raise RuntimeError('Singleton already has an initialized instance.')

        self._instance = instance
        return self

    @contextmanager
    def override_instance_for_test(self, instance: T) -> Iterator[None]:
        """A context manager that will temporarily override the value of the singleton for the purpose of a test.

        Don't use this outside of tests!"""

        prev_instance = self._instance
        self._instance = instance
        try:
            yield

        finally:
            self._instance = prev_instance
