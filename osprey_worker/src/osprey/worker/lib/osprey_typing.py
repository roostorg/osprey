from __future__ import annotations

from typing import AbstractSet, Any, Tuple, TypeVar, Union

from typing_extensions import Protocol, TypeAlias

T = TypeVar('T')
T_contra = TypeVar('T_contra', contravariant=True)
KT_co = TypeVar('KT_co', covariant=True)
VT_co = TypeVar('VT_co', covariant=True)


class SupportsItems(Protocol[KT_co, VT_co]):
    def items(self) -> AbstractSet[Tuple[KT_co, VT_co]]:
        pass


# Comparison protocols


class SupportsDunderLT(Protocol[T_contra]):
    def __lt__(self, __other: T_contra) -> bool: ...


class SupportsDunderGT(Protocol[T_contra]):
    def __gt__(self, __other: T_contra) -> bool: ...


class SupportsDunderLE(Protocol[T_contra]):
    def __le__(self, __other: T_contra) -> bool: ...


class SupportsDunderGE(Protocol[T_contra]):
    def __ge__(self, __other: T_contra) -> bool: ...


class SupportsAllComparisons(
    SupportsDunderLT[Any], SupportsDunderGT[Any], SupportsDunderLE[Any], SupportsDunderGE[Any], Protocol
): ...


SupportsRichComparison: TypeAlias = Union[SupportsDunderLT[Any], SupportsDunderGT[Any]]
SupportsRichComparisonT = TypeVar('SupportsRichComparisonT', bound=SupportsRichComparison)
