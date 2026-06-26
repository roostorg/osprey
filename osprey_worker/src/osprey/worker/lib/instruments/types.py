from __future__ import annotations

from typing import Any, TypeVar

from typing_extensions import Protocol, TypeAlias

T = TypeVar('T')


# Comparison protocols


class SupportsDunderLT(Protocol):
    def __lt__(self, __other: Any) -> bool: ...


class SupportsDunderGT(Protocol):
    def __gt__(self, __other: Any) -> bool: ...


SupportsRichComparison: TypeAlias = SupportsDunderLT | SupportsDunderGT
SupportsRichComparisonT = TypeVar('SupportsRichComparisonT', bound=SupportsRichComparison)
