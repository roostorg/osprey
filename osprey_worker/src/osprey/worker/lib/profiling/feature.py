from contextvars import ContextVar
from enum import StrEnum


class Feature(StrEnum):
    pass


_features_var: ContextVar[list[Feature] | None] = ContextVar('features', default=None)


def current_features() -> list[Feature]:
    """Return the current features"""
    return _features_var.get() or []


def set_current_features(features: list[Feature]) -> None:
    """Set the current features for the duration of the current context"""
    _features_var.set(features)
