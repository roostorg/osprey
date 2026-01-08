import json
import os
from typing import Any, Callable, Optional, Tuple

from osprey.worker.lib.config.callbacks import tracing_callback

ConfigT = dict[str, Any]
ConfigurationCallback = Callable[['Config'], None]


DEFAULT_CONFIGURATION_CALLBACKS: Tuple[ConfigurationCallback] = (tracing_callback,)


class Config:
    """Provides configuration for the rest of the osprey library."""

    def __init__(self, underlying_config_dict: Optional[ConfigT] = None):
        self._pending_configuration_callbacks: list[ConfigurationCallback] = list(DEFAULT_CONFIGURATION_CALLBACKS)
        self._underlying_config_dict: Optional[ConfigT] = None

        if underlying_config_dict is not None:
            self.configure(underlying_config_dict)

    def configure(self, underlying_config_dict: ConfigT) -> None:
        """Binds this configuration to an underlying configuration dict. Invokes all pending configuration callbacks."""
        if self._underlying_config_dict is not None:
            raise RuntimeError('Config has already been bound to an underlying config dict.')

        self._underlying_config_dict = underlying_config_dict

        # Flush pending initializations once the configuration has been applied.
        pending_configuration_callbacks = self._pending_configuration_callbacks
        self._pending_configuration_callbacks = []
        for configuration_callback in pending_configuration_callbacks:
            configuration_callback(self)

    def configure_from_env(self) -> None:
        parsed = config_from_env()
        self.configure(parsed)

    def unconfigure_for_tests(self) -> None:
        self._underlying_config_dict = None

    @property
    def _config_dict(self) -> ConfigT:
        """Returns the underlying config dict if it exists, otherwise throws an exception."""
        if self._underlying_config_dict is None:
            raise RuntimeError('Config has not yet been initialized.')

        return self._underlying_config_dict

    def expect_str(self, key: str) -> str:
        """Gets a string value from the dictionary in a type-safe way, throwing a `TypeError` if the value is not a
        string, and a `KeyError` if the key does not exist."""
        value = self._config_dict[key]
        if not isinstance(value, str):
            raise TypeError(f'Type of config[{key!r}] is not a str, but a {type(value)}')

        return value

    def get_str(self, key: str, default: str) -> str:
        """Like `expect_str`, returning a default value if the key does not exist.
        Will still throw a type error, if the value was not a string."""
        try:
            return self.expect_str(key)
        except KeyError:
            return default

    def get_optional_str(self, key: str) -> Optional[str]:
        """Like `expect_str`, but returns None if the key does not exist.
        Will still throw a type error if the value was present and not a string."""
        try:
            return self.expect_str(key)
        except KeyError:
            return None

    def expect_float(self, key: str) -> float:
        """Gets a float value from the dictionary in a type-safe way, throwing a `TypeError` if the value is not a
        float, and a `KeyError` if the key does not exist."""
        value = self._config_dict[key]
        # Let's try and parse the value as an integer.
        if isinstance(value, str):
            try:
                value = float(value)
            except ValueError:
                raise TypeError(
                    f'Type of config[{key!r}] is not a float, but a {type(value)} '
                    f'that cannot be interpreted as a string'
                )

        if not isinstance(value, float):
            raise TypeError(f'Type of config[{key!r}] is not a int, but a {type(value)}')

        return value

    def get_float(self, key: str, default: float) -> float:
        """Like `expect_float`, returning a default value if the key does not exist.
        Will still throw a type error, if the value was not a string."""
        try:
            return self.expect_float(key)
        except KeyError:
            return default

    def expect_type(self, key: str, expected_type: type) -> Any:
        """Gets a value of the expected type from the dictionary in a type-safe way,
        throwing a `TypeError` if the value is not of the expected type, and a `KeyError` if the key does not exist."""
        value = self._config_dict[key]

        if isinstance(value, str):
            try:
                value = expected_type(value)
            except ValueError:
                raise TypeError(
                    f'Type of config[{key!r}] is not a {expected_type.__name__}, but a {type(value).__name__} '
                    f'that cannot be interpreted as a string'
                )
        if not isinstance(value, expected_type):
            raise TypeError(f'Type of config[{key!r}] is not a {expected_type.__name__}, but a {type(value).__name__}')

        return value

    def get_type(self, key: str, default_value: Any, expected_type: type) -> Any:
        """Like `expect_type`, returning a default value if the key does not exist.
        Will still throw a type error, if the value was not of the expected type."""
        try:
            return self.expect_type(key, expected_type)
        except KeyError:
            return default_value

    def expect_bool(self, key: str) -> bool:
        """Gets a boolean value from the dictionary in a type-safe way, throwing a `TypeError` if the value is not a
        bool, and a `KeyError` if the key does not exist."""
        value = self._config_dict[key]
        if isinstance(value, str):
            value_lower = value.lower()
            if value_lower in ('true', 'false'):
                value = value_lower == 'true'
            else:
                raise TypeError(
                    f'Type of config[{key!r}] is not a bool, but a {type(value)} '
                    f'that cannot be interpreted as a bool (e.g: "true" or "false")'
                )

        if not isinstance(value, bool):
            raise TypeError(f'Type of config[{key!r}] is not a bool, but a {type(value)}')

        return value

    def get_bool(self, key: str, default: bool) -> bool:
        """Like `expect_bool`, returning a default value if the key does not exist.
        Will still throw a type error, if the value was not a bool."""
        try:
            return self.expect_bool(key)
        except KeyError:
            return default

    def expect_int(self, key: str) -> int:
        """Gets a int value from the dictionary in a type-safe way, throwing a `TypeError` if the value is not a
        int, and a `KeyError` if the key does not exist."""
        value = self._config_dict[key]
        # Let's try and parse the value as an integer.
        if isinstance(value, str):
            try:
                value = int(value)
            except ValueError:
                raise TypeError(
                    f'Type of config[{key!r}] is not a int, but a {type(value)} that cannot be interpreted as a string'
                )

        if not isinstance(value, int):
            raise TypeError(f'Type of config[{key!r}] is not a int, but a {type(value)}')

        return value

    def get_int(self, key: str, default: int) -> int:
        """Like `expect_int, returning a default value if the key does not exist.
        Will still throw a type error, if the value was not a int."""
        try:
            return self.expect_int(key)
        except KeyError:
            return default

    def expect_str_list(self, key: str) -> list[str]:
        """Gets a list[str] value from the dictionary in a type-safe way, throwing a `TypeError` if the value is not a
        list[str], and a `KeyError` if the key does not exist."""
        value = self._config_dict[key]
        if not isinstance(value, list):
            raise TypeError(f'Type of config[{key!r}] is not a list, but a {type(value)}')

        for i, it in enumerate(value):
            if not isinstance(it, str):
                raise TypeError(f'config[{key!r}][{i}] is not a str, but a {type(it)}')

        return value

    def get_str_list(self, key: str, default: list[str]) -> list[str]:
        """Like `get_str`, returning a default value if the key does not exist.
        Will still throw a type error, if the value was not a list[str]."""
        try:
            return self.expect_str_list(key)
        except KeyError:
            return default

    def register_configuration_callback(self, configuration_callback: ConfigurationCallback) -> None:
        """Invokes a callback when the the underlying configuration is available."""
        if self._underlying_config_dict is not None:
            configuration_callback(self)
        else:
            self._pending_configuration_callbacks.append(configuration_callback)

    @property
    def testing(self) -> bool:
        """Returns whether or not the config is in "TESTING" mode, which means we are running unit tests."""
        return self.get_bool('TESTING', False)

    @property
    def debug(self) -> bool:
        """Returns whether or not the app is in "DEBUG" mode, which means we are running in local development."""
        return self.get_bool('DEBUG', False)

    def __getitem__(self, item: str) -> Any:
        return self._config_dict[item]


def config_from_env(
    env: Optional[dict[str, str]] = None, key_filter: Optional[Callable[[str], bool]] = None
) -> dict[str, Any]:
    """Creates a config dictionary from the process environment. Tries to parse json-like values as JSON, meaning,
    if you had a config value that looks like a JSON object (starts with `{`, `[` or `"`), it will try to be interpreted
    as JSON, and fail loudly if it can't.

    By default will use `os.environ` as the environment, but any ol' dictionary will do.

    An optional `key_filter` can be provided, that will take the key, and return True if the key should be used in the
    config, or false if it shouldn't. For example, you can make the key filter be: `{'foo', 'bar'}.__contains__`.
    """
    env_: dict[str, str] = env if env is not None else os.environ.copy()

    config = {}

    for k, v in env_.items():
        if key_filter and not key_filter(k):
            continue

        # This is a hint to try and parse the value as JSON.
        if v.startswith(('{', '[', '"')):
            try:
                v = json.loads(v)
            except json.JSONDecodeError as e:
                raise ValueError(f'Could not parse key={k} as JSON (error: {e!r})')

        config[k] = v

    return config
