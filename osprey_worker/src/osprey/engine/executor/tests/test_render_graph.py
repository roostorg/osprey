import copy
import json
from typing import Any, Dict, Set

import pytest
from osprey.engine.ast_validator.validation_context import ValidatedSources
from osprey.engine.ast_validator.validator_registry import ValidatorRegistry
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.ast_validator.validators.validate_dynamic_calls_have_annotated_rvalue import (
    ValidateDynamicCallsHaveAnnotatedRValue,
)
from osprey.engine.conftest import RunValidationFunction
from osprey.engine.stdlib import get_config_registry

from ..execution_graph import ExecutionGraph, compile_execution_graph
from ..execution_visualizer import _render_graph

pytestmark = [
    pytest.mark.use_validators([ValidateCallKwargs, ValidateDynamicCallsHaveAnnotatedRValue, UniqueStoredNames]),
    pytest.mark.use_osprey_stdlib,
]

_labels_raw = {
    'test_label_one': {'valid_for': ['User'], 'connotation': 'negative', 'description': 'Hello world 1!'},
    'test_label_two': {'valid_for': ['User'], 'connotation': 'neutral', 'description': 'Hello world 2!'},
    'test_label_three': {'valid_for': ['User'], 'connotation': 'positive', 'description': 'Hello world 3!'},
    'test_label_four': {'valid_for': ['User'], 'connotation': 'neutral', 'description': 'Hello world 4!'},
    'test_label_five': {'valid_for': ['User'], 'connotation': 'negative', 'description': 'Hello world 5!'},
}

_base_sources_dict = {'config.yaml': json.dumps({'labels': _labels_raw})}

_models_dict = {
    'models/action_name.sml': """
        ActionName = GetActionName()
    """,
    'models/user.sml': """
        Import(
            rules=[
                'models/user_id.sml',
            ]
        )
    """,
    'models/user_id.sml': """
        UserId: Entity[int] = EntityJson(type='User', path='$.user.id', coerce_type=True)
    """,
}

main_file = """
    Import(
        rules=[
            'models/action_name.sml',
        ]
    )

    Require(rule=f'actions/{ActionName}.sml')
"""

basic_action_file = """
    Import(
        rules=[
            'models/user.sml',
            'models/user_id.sml',
        ]
    )

    Name = "test_name_for_rule_a"

    Rule_A = Rule(
        when_all=[True],
        description=f'Test basic rule {Name}',
    )

    WhenRules(
        rules_any=[Rule_A],
        then=[LabelAdd(entity=UserId, label='test_label_one')],
    )

    Rule_B = Rule(
        when_all=[HasLabel(entity=UserId, label='test_label_one')],
        description='User is hellbanned',
    )

    WhenRules(
        rules_any=[Rule_B],
        then=[LabelAdd(entity=UserId, label='test_label_two')],
    )
"""

remedy_action_file = """
    Import(
        rules=[
            'models/user.sml',
            'models/user_id.sml',
        ]
    )

    Has_Label_Three = Rule(
        when_all=[
            HasLabel(entity=UserId, label='test_label_three'),
        ],
        description=f'User has label three',
    )

    Has_Label_Four= Rule(
        when_all=[
            HasLabel(entity=UserId, label='test_label_four'),
        ],
        description=f'User has label four',
    )

    Has_Label_Five = Rule(
        when_all=[
            HasLabel(entity=UserId, label='test_label_five'),
        ],
        description=f'User has label five',
    )

    WhenRules(
        rules_any=[Has_Label_Three, Has_Label_Four, Has_Label_Five],
        then=[
            LabelRemove(entity=UserId, label='test_label_three'),
            LabelRemove(entity=UserId, label='test_label_four'),
            LabelRemove(entity=UserId, label='test_label_five'),
        ],
    )
"""

config = {
    **_base_sources_dict,
    **_models_dict,
    'main.sml': main_file,
    'actions/basic_action.sml': basic_action_file,
    'actions/remedy_action.sml': remedy_action_file,
}

_all_action_names = {
    key[len('actions/') : len(key) - len('.sml')]  # noqa: E203
    for key in config.keys()
    if isinstance(key, str) and key.startswith('actions/') and key.endswith('.sml')
}
_all_label_names = {key for key in _labels_raw.keys() if isinstance(key, str)}


@pytest.fixture
def execution_graph(run_validation: RunValidationFunction) -> ExecutionGraph:
    """
    Compiles an ExecutionGraph based on the above Osprey Rules configs
    """
    config_validator = get_config_registry().get_validator()
    validator_registry = ValidatorRegistry.get_instance().instance_with_additional_validators(config_validator)
    validated_sources: ValidatedSources = run_validation(config, validator_registry=validator_registry)
    execution_graph = compile_execution_graph(validated_sources)
    return execution_graph


def get_labels_view_graph(
    execution_graph: ExecutionGraph, label_names: Set[str], show_upstream: bool = False, show_downstream: bool = True
) -> Dict[str, Any]:
    """
    Returns a graph of the specified label_names
    """
    render = _render_graph(
        all_action_names=_all_action_names,
        all_label_names=_all_label_names,
        execution_graph=execution_graph,
        label_names_to_render=label_names,
        show_label_upstream=show_upstream,
        show_label_downstream=show_downstream,
    )
    val: Dict[str, Any] = json.loads(json.dumps(render.data))
    return val


def get_actions_view_graph(execution_graph: ExecutionGraph, action_names: Set[str]) -> Dict[str, Any]:
    """
    Returns a graph of the specified action_names
    """
    render = _render_graph(
        all_action_names=_all_action_names,
        all_label_names=_all_label_names,
        execution_graph=execution_graph,
        action_names_to_render=action_names,
    )
    val: Dict[str, Any] = json.loads(json.dumps(render.data))
    return val


def assert_graphs_are_equal(actual_graph: Dict[str, Any], expected_graph: Dict[str, Any]) -> None:
    """
    Asserts that the two supplied graphs are equal. Note that actual_graph and expected_graph will have different
    unique IDs but the same data-- This is expected behavior, hence why this logic is necessary to ensure a fair
    comparison.
    """

    def remove_references_to_ids(graph: Dict[str, Any]) -> None:
        """
        Removes all ID fields and replaces all node IDs inside edges with their actual node data
        """
        for node in graph['nodes']:
            node_id = node['id']
            del node['id']
            for edge in graph['edges']:
                if edge['source'] == node_id:
                    edge['source'] = node
                if edge['target'] == node_id:
                    edge['target'] = node

    # Copy so that we don't modify any input data unexpectedly
    actual_graph_copy = copy.deepcopy(actual_graph)
    expected_graph_copy = copy.deepcopy(expected_graph)
    remove_references_to_ids(actual_graph_copy)
    remove_references_to_ids(expected_graph_copy)
    assert actual_graph_copy == expected_graph_copy


def test_downstream_labels_view(execution_graph: ExecutionGraph) -> None:
    label_one_graph = get_labels_view_graph(
        execution_graph, label_names={'test_label_one'}, show_upstream=False, show_downstream=True
    )
    assert_graphs_are_equal(
        label_one_graph,
        {
            'nodes': [
                {
                    'id': 40,
                    'name': "HasLabel: 'test_label_one' on entity 'UserId'",
                    'type': 'Label',
                    'file_path': 'actions/basic_action.sml',
                    'value': ['test_label_one', 'UserId'],
                    'label_name': 'test_label_one',
                    'entity_name': 'UserId',
                    'label_type': 'HasLabel',
                    'num_children': 2,
                },
                {
                    'id': 38,
                    'name': 'Rule: Rule_B',
                    'type': 'Rule',
                    'file_path': 'actions/basic_action.sml',
                    'value': 'Rule_B',
                    'num_children': 1,
                },
                {
                    'id': 45,
                    'name': "LabelAdd: 'test_label_two' on entity 'UserId'",
                    'type': 'Label',
                    'file_path': 'actions/basic_action.sml',
                    'value': ['test_label_two', 'UserId'],
                    'label_name': 'test_label_two',
                    'entity_name': 'UserId',
                    'label_type': 'LabelAdd',
                    'num_children': 0,
                },
                {
                    'id': 37,
                    'name': "LabelAdd: 'test_label_one' on entity 'UserId'",
                    'type': 'Label',
                    'file_path': 'actions/basic_action.sml',
                    'value': ['test_label_one', 'UserId'],
                    'label_name': 'test_label_one',
                    'entity_name': 'UserId',
                    'label_type': 'LabelAdd',
                    'num_children': 0,
                },
            ],
            'edges': [
                {'source': 40, 'target': 38, 'color': '#0000FF'},
                {'source': 38, 'target': 45, 'color': '#E6E6E6'},
            ],
        },
    )
    label_two_graph = get_labels_view_graph(
        execution_graph, label_names={'test_label_two'}, show_upstream=False, show_downstream=True
    )
    assert_graphs_are_equal(
        label_two_graph,
        {
            'nodes': [
                {
                    'id': 45,
                    'name': "LabelAdd: 'test_label_two' on entity 'UserId'",
                    'type': 'Label',
                    'file_path': 'actions/basic_action.sml',
                    'value': ['test_label_two', 'UserId'],
                    'label_name': 'test_label_two',
                    'entity_name': 'UserId',
                    'label_type': 'LabelAdd',
                    'num_children': 0,
                }
            ],
            'edges': [],
        },
    )


def test_upstream_labels_view(execution_graph: ExecutionGraph) -> None:
    label_one_graph = get_labels_view_graph(
        execution_graph,
        label_names={'test_label_one'},
        show_upstream=True,
        show_downstream=False,
    )
    assert_graphs_are_equal(
        label_one_graph,
        {
            'nodes': [
                {
                    'id': 8,
                    'name': 'Rule: Rule_A',
                    'type': 'Rule',
                    'file_path': 'actions/basic_action.sml',
                    'value': 'Rule_A',
                    'num_children': 1,
                },
                {
                    'id': 20,
                    'name': "HasLabel: 'test_label_one' on entity 'UserId'",
                    'type': 'Label',
                    'file_path': 'actions/basic_action.sml',
                    'value': ['test_label_one', 'UserId'],
                    'label_name': 'test_label_one',
                    'entity_name': 'UserId',
                    'label_type': 'HasLabel',
                    'num_children': 0,
                },
                {
                    'id': 15,
                    'name': "LabelAdd: 'test_label_one' on entity 'UserId'",
                    'type': 'Label',
                    'file_path': 'actions/basic_action.sml',
                    'value': ['test_label_one', 'UserId'],
                    'label_name': 'test_label_one',
                    'entity_name': 'UserId',
                    'label_type': 'LabelAdd',
                    'num_children': 0,
                },
            ],
            'edges': [{'source': 8, 'target': 15, 'color': '#E6E6E6'}],
        },
    )
    label_two_graph = get_labels_view_graph(
        execution_graph,
        label_names={'test_label_two'},
        show_upstream=True,
        show_downstream=False,
    )
    assert_graphs_are_equal(
        label_two_graph,
        {
            'nodes': [
                {
                    'id': 40,
                    'name': "HasLabel: 'test_label_one' on entity 'UserId'",
                    'type': 'Label',
                    'file_path': 'actions/basic_action.sml',
                    'value': ['test_label_one', 'UserId'],
                    'label_name': 'test_label_one',
                    'entity_name': 'UserId',
                    'label_type': 'HasLabel',
                    'num_children': 2,
                },
                {
                    'id': 38,
                    'name': 'Rule: Rule_B',
                    'type': 'Rule',
                    'file_path': 'actions/basic_action.sml',
                    'value': 'Rule_B',
                    'num_children': 1,
                },
                {
                    'id': 45,
                    'name': "LabelAdd: 'test_label_two' on entity 'UserId'",
                    'type': 'Label',
                    'file_path': 'actions/basic_action.sml',
                    'value': ['test_label_two', 'UserId'],
                    'label_name': 'test_label_two',
                    'entity_name': 'UserId',
                    'label_type': 'LabelAdd',
                    'num_children': 0,
                },
            ],
            'edges': [
                {'source': 40, 'target': 38, 'color': '#0000FF'},
                {'source': 38, 'target': 45, 'color': '#E6E6E6'},
            ],
        },
    )


def test_actions_view(execution_graph: ExecutionGraph) -> None:
    basic_action_graph = get_actions_view_graph(execution_graph, {'basic_action'})
    assert_graphs_are_equal(
        basic_action_graph,
        {
            'nodes': [
                {
                    'id': 20,
                    'name': "HasLabel: 'test_label_one' on entity 'UserId'",
                    'type': 'Label',
                    'file_path': 'actions/basic_action.sml',
                    'value': ['test_label_one', 'UserId'],
                    'label_name': 'test_label_one',
                    'entity_name': 'UserId',
                    'label_type': 'HasLabel',
                    'num_children': 2,
                },
                {
                    'id': 18,
                    'name': 'Rule: Rule_B',
                    'type': 'Rule',
                    'file_path': 'actions/basic_action.sml',
                    'value': 'Rule_B',
                    'num_children': 1,
                },
                {
                    'id': 8,
                    'name': 'Rule: Rule_A',
                    'type': 'Rule',
                    'file_path': 'actions/basic_action.sml',
                    'value': 'Rule_A',
                    'num_children': 1,
                },
                {
                    'id': 25,
                    'name': "LabelAdd: 'test_label_two' on entity 'UserId'",
                    'type': 'Label',
                    'file_path': 'actions/basic_action.sml',
                    'value': ['test_label_two', 'UserId'],
                    'label_name': 'test_label_two',
                    'entity_name': 'UserId',
                    'label_type': 'LabelAdd',
                    'num_children': 0,
                },
                {
                    'id': 15,
                    'name': "LabelAdd: 'test_label_one' on entity 'UserId'",
                    'type': 'Label',
                    'file_path': 'actions/basic_action.sml',
                    'value': ['test_label_one', 'UserId'],
                    'label_name': 'test_label_one',
                    'entity_name': 'UserId',
                    'label_type': 'LabelAdd',
                    'num_children': 0,
                },
            ],
            'edges': [
                {'source': 20, 'target': 18, 'color': '#0000FF'},
                {'source': 18, 'target': 25, 'color': '#E6E6E6'},
                {'source': 8, 'target': 15, 'color': '#E6E6E6'},
            ],
        },
    )

    remedy_action_graph = get_actions_view_graph(execution_graph, {'remedy_action'})
    assert_graphs_are_equal(
        remedy_action_graph,
        {
            'nodes': [
                {
                    'id': 18,
                    'name': "HasLabel: 'test_label_five' on entity 'UserId'",
                    'type': 'Label',
                    'file_path': 'actions/remedy_action.sml',
                    'value': ['test_label_five', 'UserId'],
                    'label_name': 'test_label_five',
                    'entity_name': 'UserId',
                    'label_type': 'HasLabel',
                    'num_children': 4,
                },
                {
                    'id': 14,
                    'name': "HasLabel: 'test_label_four' on entity 'UserId'",
                    'type': 'Label',
                    'file_path': 'actions/remedy_action.sml',
                    'value': ['test_label_four', 'UserId'],
                    'label_name': 'test_label_four',
                    'entity_name': 'UserId',
                    'label_type': 'HasLabel',
                    'num_children': 4,
                },
                {
                    'id': 8,
                    'name': "HasLabel: 'test_label_three' on entity 'UserId'",
                    'type': 'Label',
                    'file_path': 'actions/remedy_action.sml',
                    'value': ['test_label_three', 'UserId'],
                    'label_name': 'test_label_three',
                    'entity_name': 'UserId',
                    'label_type': 'HasLabel',
                    'num_children': 4,
                },
                {
                    'id': 16,
                    'name': 'Rule: Has_Label_Five',
                    'type': 'Rule',
                    'file_path': 'actions/remedy_action.sml',
                    'value': 'Has_Label_Five',
                    'num_children': 3,
                },
                {
                    'id': 12,
                    'name': 'Rule: Has_Label_Four',
                    'type': 'Rule',
                    'file_path': 'actions/remedy_action.sml',
                    'value': 'Has_Label_Four',
                    'num_children': 3,
                },
                {
                    'id': 6,
                    'name': 'Rule: Has_Label_Three',
                    'type': 'Rule',
                    'file_path': 'actions/remedy_action.sml',
                    'value': 'Has_Label_Three',
                    'num_children': 3,
                },
                {
                    'id': 25,
                    'name': "LabelRemove: 'test_label_five' on entity 'UserId'",
                    'type': 'Label',
                    'file_path': 'actions/remedy_action.sml',
                    'value': ['test_label_five', 'UserId'],
                    'label_name': 'test_label_five',
                    'entity_name': 'UserId',
                    'label_type': 'LabelRemove',
                    'num_children': 0,
                },
                {
                    'id': 24,
                    'name': "LabelRemove: 'test_label_four' on entity 'UserId'",
                    'type': 'Label',
                    'file_path': 'actions/remedy_action.sml',
                    'value': ['test_label_four', 'UserId'],
                    'label_name': 'test_label_four',
                    'entity_name': 'UserId',
                    'label_type': 'LabelRemove',
                    'num_children': 0,
                },
                {
                    'id': 23,
                    'name': "LabelRemove: 'test_label_three' on entity 'UserId'",
                    'type': 'Label',
                    'file_path': 'actions/remedy_action.sml',
                    'value': ['test_label_three', 'UserId'],
                    'label_name': 'test_label_three',
                    'entity_name': 'UserId',
                    'label_type': 'LabelRemove',
                    'num_children': 0,
                },
            ],
            'edges': [
                {'source': 18, 'target': 16, 'color': '#0000FF'},
                {'source': 14, 'target': 12, 'color': '#0000FF'},
                {'source': 8, 'target': 6, 'color': '#0000FF'},
                {'source': 16, 'target': 23, 'color': '#E6E6E6'},
                {'source': 16, 'target': 24, 'color': '#E6E6E6'},
                {'source': 16, 'target': 25, 'color': '#E6E6E6'},
                {'source': 12, 'target': 23, 'color': '#E6E6E6'},
                {'source': 12, 'target': 24, 'color': '#E6E6E6'},
                {'source': 12, 'target': 25, 'color': '#E6E6E6'},
                {'source': 6, 'target': 23, 'color': '#E6E6E6'},
                {'source': 6, 'target': 24, 'color': '#E6E6E6'},
                {'source': 6, 'target': 25, 'color': '#E6E6E6'},
            ],
        },
    )
