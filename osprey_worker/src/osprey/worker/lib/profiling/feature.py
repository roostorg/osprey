from contextvars import ContextVar
from enum import StrEnum
from typing import List, Optional


class Feature(StrEnum):
    pass


_features_var: ContextVar[Optional[List[Feature]]] = ContextVar('features', default=None)


def current_features() -> List[Feature]:
    """Return the current features"""
    return _features_var.get() or []


def set_current_features(features: List[Feature]) -> None:
    """Set the current features for the duration of the current context"""
    _features_var.set(features)
