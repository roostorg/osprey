"""`__entity_label_mutations` and `__verdicts` are ARRAY columns in Druid, which rejects pattern filters such as
`like` on them ("Predicate does not support ARRAY types"), so the label and verdict query UDFs must use filters
that Druid supports for arrays."""

import json

import pytest
from osprey.engine.ast_validator.validators.imports_must_not_have_cycles import ImportsMustNotHaveCycles
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.ast_validator.validators.validate_dynamic_calls_have_annotated_rvalue import (
    ValidateDynamicCallsHaveAnnotatedRValue,
)
from osprey.engine.ast_validator.validators.validate_static_types import ValidateStaticTypes
from osprey.engine.ast_validator.validators.variables_must_be_defined import VariablesMustBeDefined
from osprey.engine.conftest import RunValidationFunction
from osprey.engine.query_language import parse_query_to_validated_ast
from osprey.engine.query_language.ast_druid_translator import DruidQueryTransformer

pytestmark = [
    pytest.mark.use_standard_rules_validators(),
    pytest.mark.use_validators(
        [
            UniqueStoredNames,
            ValidateStaticTypes,
            ValidateCallKwargs,
            ImportsMustNotHaveCycles,
            ValidateDynamicCallsHaveAnnotatedRValue,
            VariablesMustBeDefined,
        ]
    ),
]


def contains(column: str, value: str) -> dict[str, object]:
    return {
        'type': 'and',
        'fields': [
            {
                'type': 'arrayContainsElement',
                'column': column,
                'elementMatchValueType': 'STRING',
                'elementMatchValue': value,
            },
            {'type': 'not', 'field': {'type': 'null', 'column': column}},
        ],
    }


@pytest.mark.parametrize(
    'query, expected_filter',
    [
        (
            'DidAddLabel(entity_type="User", label_name="meow")',
            contains('__entity_label_mutations', 'User/meow/1'),
        ),
        (
            'DidRemoveLabel(entity_type="User", label_name="meow")',
            contains('__entity_label_mutations', 'User/meow/0'),
        ),
        (
            'not DidAddLabel(entity_type="User", label_name="meow")',
            {'type': 'not', 'field': contains('__entity_label_mutations', 'User/meow/1')},
        ),
        ('DidDeclareVerdict(verdict="ban")', contains('__verdicts', 'ban')),
        ('not DidDeclareVerdict(verdict="ban")', {'type': 'not', 'field': contains('__verdicts', 'ban')}),
        (
            'DidAddLabel(entity_type="User", label_name="meow") or DidDeclareVerdict(verdict="ban")',
            {
                'type': 'or',
                'fields': [contains('__entity_label_mutations', 'User/meow/1'), contains('__verdicts', 'ban')],
            },
        ),
        (
            'DidAddLabel(entity_type="User", label_name="meow") and not DidDeclareVerdict(verdict="ban")',
            {
                'type': 'and',
                'fields': [
                    contains('__entity_label_mutations', 'User/meow/1'),
                    {'type': 'not', 'field': contains('__verdicts', 'ban')},
                ],
            },
        ),
    ],
)
def test_label_and_verdict_queries_use_array_filters(
    run_validation: RunValidationFunction, query: str, expected_filter: dict[str, object]
) -> None:
    validated_sources = parse_query_to_validated_ast(
        query,
        run_validation({'main.sml': '', 'config.yaml': json.dumps({'labels': {'meow': {'valid_for': ['User']}}})}),
    )

    assert DruidQueryTransformer(validated_sources=validated_sources).transform() == {'filter': expected_filter}
