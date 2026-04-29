from __future__ import annotations

from typing import Any, Dict

from osprey.engine.query_language.filter_ir import (
    ArrayContainsFilter,
    BooleanFilter,
    ComparisonFilter,
    ComparisonOperator,
    ContainsFilter,
    DruidRawFilter,
    FeatureRef,
    FilterExpression,
    InFilter,
    LikeFilter,
    LiteralValue,
    NotFilter,
    RegexFilter,
)


class DruidFilterTransformException(Exception):
    """Some error happened while trying to transform a filter IR node into a Druid filter."""


class DruidFilterTranslator:
    def transform(self, filter_ir: FilterExpression) -> Dict[str, Any]:
        return self._transform(filter_ir)

    def _transform(self, filter_ir: FilterExpression) -> Dict[str, Any]:
        if isinstance(filter_ir, DruidRawFilter):
            return dict(filter_ir.filter)
        elif isinstance(filter_ir, BooleanFilter):
            return {'type': filter_ir.operator.value, 'fields': [self._transform(v) for v in filter_ir.fields]}
        elif isinstance(filter_ir, NotFilter):
            return {'type': 'not', 'field': self._transform(filter_ir.field)}
        elif isinstance(filter_ir, ComparisonFilter):
            return self._transform_comparison(filter_ir)
        elif isinstance(filter_ir, ContainsFilter):
            return {
                'type': 'search',
                'dimension': filter_ir.feature.name,
                'query': {
                    'type': 'contains' if filter_ir.case_sensitive else 'insensitive_contains',
                    'value': filter_ir.value.value,
                },
            }
        elif isinstance(filter_ir, InFilter):
            return {'type': 'in', 'dimension': filter_ir.feature.name, 'values': list(filter_ir.values)}
        elif isinstance(filter_ir, RegexFilter):
            return {'type': 'regex', 'dimension': filter_ir.feature.name, 'pattern': filter_ir.pattern}
        elif isinstance(filter_ir, LikeFilter):
            return {'type': 'like', 'dimension': filter_ir.feature.name, 'pattern': filter_ir.pattern}
        elif isinstance(filter_ir, ArrayContainsFilter):
            return {
                'type': 'arrayContainsElement',
                'column': filter_ir.feature.name,
                'elementMatchType': get_druid_array_element_type(filter_ir.value.value),
                'elementMatchValue': filter_ir.value.value,
            }

        raise DruidFilterTransformException(f'Unknown filter IR type: {filter_ir.__class__.__name__}')

    def _transform_comparison(self, filter_ir: ComparisonFilter) -> Dict[str, Any]:
        if isinstance(filter_ir.left, FeatureRef) and isinstance(filter_ir.right, FeatureRef):
            column_comparison = {
                'type': 'columnComparison',
                'dimensions': [filter_ir.left.name, filter_ir.right.name],
            }
            if filter_ir.operator == ComparisonOperator.EQUALS:
                return column_comparison
            elif filter_ir.operator == ComparisonOperator.NOT_EQUALS:
                return {'type': 'not', 'field': column_comparison}
            else:
                raise DruidFilterTransformException(
                    'When comparing two features, only the `==` and `!=` operators are supported'
                )

        dimension, value = get_feature_and_literal(filter_ir)
        selector_value = normalize_druid_selector_value(value.value)

        if filter_ir.operator == ComparisonOperator.EQUALS:
            return {'type': 'selector', 'dimension': dimension.name, 'value': selector_value}
        elif filter_ir.operator == ComparisonOperator.NOT_EQUALS:
            return {
                'type': 'not',
                'field': {'type': 'selector', 'dimension': dimension.name, 'value': selector_value},
            }

        bound_query = {
            'type': 'bound',
            'dimension': dimension.name,
            'ordering': get_value_bound_ordering(value.value),
            **get_druid_bound_query_props(filter_ir.operator, value.value),
        }

        return {
            'type': 'and',
            'fields': [
                {'type': 'not', 'field': {'type': 'selector', 'dimension': dimension.name, 'value': None}},
                bound_query,
            ],
        }


def get_feature_and_literal(filter_ir: ComparisonFilter) -> tuple[FeatureRef, LiteralValue]:
    if isinstance(filter_ir.left, FeatureRef) and isinstance(filter_ir.right, LiteralValue):
        return filter_ir.left, filter_ir.right
    elif isinstance(filter_ir.left, LiteralValue) and isinstance(filter_ir.right, FeatureRef):
        return filter_ir.right, filter_ir.left

    raise DruidFilterTransformException('Comparison must contain one feature and one literal, or two features')


def get_druid_bound_query_props(operator: ComparisonOperator, comparison_value: Any) -> Dict[str, Any]:
    """Get the correct query properties for the various type of `bound` filters."""

    if operator == ComparisonOperator.LESS_THAN:
        return {'upper': comparison_value, 'upperStrict': True}
    elif operator == ComparisonOperator.LESS_THAN_EQUALS:
        return {'upper': comparison_value}
    elif operator == ComparisonOperator.GREATER_THAN:
        return {'lower': comparison_value, 'lowerStrict': True}
    elif operator == ComparisonOperator.GREATER_THAN_EQUALS:
        return {'lower': comparison_value}
    else:
        raise DruidFilterTransformException('Unknown Binary Comparator')


def get_value_bound_ordering(value: Any) -> str:
    """Given a value, return the appropriate comparator for the value to be used in a bound filter."""

    if isinstance(value, (int, float)):
        return 'numeric'
    elif isinstance(value, str):
        return 'lexicographic'

    raise TypeError(f'Cannot compare a {value.__class__.__name__}')


def get_druid_array_element_type(value: Any) -> str:
    if isinstance(value, bool):
        return 'LONG'
    elif isinstance(value, int):
        return 'LONG'
    elif isinstance(value, float):
        return 'DOUBLE'
    return 'STRING'


def normalize_druid_selector_value(value: Any) -> Any:
    if isinstance(value, bool):
        return 1 if value else 0
    return value
