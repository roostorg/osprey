from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Mapping, TypeAlias


class BooleanOperator(StrEnum):
    AND = 'and'
    OR = 'or'


class ComparisonOperator(StrEnum):
    EQUALS = 'equals'
    NOT_EQUALS = 'not_equals'
    LESS_THAN = 'less_than'
    LESS_THAN_EQUALS = 'less_than_equals'
    GREATER_THAN = 'greater_than'
    GREATER_THAN_EQUALS = 'greater_than_equals'


@dataclass(frozen=True)
class FeatureRef:
    name: str


@dataclass(frozen=True)
class LiteralValue:
    value: Any


FilterValue: TypeAlias = FeatureRef | LiteralValue


@dataclass(frozen=True)
class BooleanFilter:
    operator: BooleanOperator
    fields: tuple['FilterExpression', ...]


@dataclass(frozen=True)
class NotFilter:
    field: 'FilterExpression'


@dataclass(frozen=True)
class ComparisonFilter:
    left: FilterValue
    operator: ComparisonOperator
    right: FilterValue


@dataclass(frozen=True)
class ContainsFilter:
    feature: FeatureRef
    value: LiteralValue
    case_sensitive: bool = False


@dataclass(frozen=True)
class InFilter:
    feature: FeatureRef
    values: tuple[Any, ...]


@dataclass(frozen=True)
class RegexFilter:
    feature: FeatureRef
    pattern: str


@dataclass(frozen=True)
class LikeFilter:
    feature: FeatureRef
    pattern: str


@dataclass(frozen=True)
class ArrayContainsFilter:
    feature: FeatureRef
    value: LiteralValue


@dataclass(frozen=True)
class DruidRawFilter:
    """Compatibility wrapper for legacy query UDFs that only provide Druid JSON."""

    filter: Mapping[str, Any]


FilterExpression: TypeAlias = (
    BooleanFilter
    | NotFilter
    | ComparisonFilter
    | ContainsFilter
    | InFilter
    | RegexFilter
    | LikeFilter
    | ArrayContainsFilter
    | DruidRawFilter
)


def and_filters(fields: tuple[FilterExpression, ...]) -> FilterExpression | None:
    if not fields:
        return None
    if len(fields) == 1:
        return fields[0]
    return BooleanFilter(operator=BooleanOperator.AND, fields=fields)
