import copy
import enum
import pathlib
import tempfile
from typing import Any, Type

from graphviz import Digraph
from osprey.engine.ast.grammar import (
    Assign,
    ASTNode,
    BinaryComparison,
    BinaryOperation,
    Boolean,
    BooleanOperation,
    Call,
    FormatString,
    Literal,
    Load,
    Number,
    String,
)
from osprey.engine.executor.graph_data import GraphData, LabelType, Node, NodeType
from osprey.engine.stdlib.configs.labels_config import LabelsConfig
from osprey.worker.lib.singletons import ENGINE

from .dependency_chain import DependencyChain
from .execution_graph import ExecutionGraph

debug: bool = False


def print_debug(input: str) -> None:
    if debug:
        print(input)


class GraphViewType(enum.Enum):
    LABELS_VIEW = 0
    ACTIONS_VIEW = 1


def _graph_data_pre_processing(data: GraphData) -> None:
    """
    Basic preprocessing to remove some of the easy nodes to compact
    """
    for node_id in data.get_node_ids():
        node = data.get_node(node_id)
        node_value = node.data['value']
        if node.type in [NodeType.NUMBER] or (node.type == NodeType.CALL and node_value == 'Import'):
            """
            Number nodes are always pointing to other nodes (like Lists or comparison operations) so we can safely
            delete them without worrying about losing path meaning. Same goes for Import statements-- they extend off of
            the File nodes for the sake of code execution clarity, but for the sake of rules visualization they serve no
            purpose.
            """
            data.delete_node(node_id)
        elif node.type in [NodeType.LIST, NodeType.BOOLEAN, NodeType.ASSIGN] or (
            node.type == NodeType.FILE and '{ActionName}' in node.data['name']
        ):
            """
            All of these nodes seemed to be directly involved in the pathing for rules. However, they themselves dont
            really provide good info, so we want to delete them while keeping their path meaning.
            """
            data.delete_node_and_preserve_edges(node_id)
        elif node.type in [
            NodeType.FORMAT_STRING,
            NodeType.STRING,
            NodeType.BOOLEAN_OPERATION,
            NodeType.UNARY_OPERATION,
            NodeType.NONE_,
            NodeType.BINARY_COMPARISON,
        ]:
            """
            This final cleanup stage is a way to *slightly more intelligently* delete nodes without risking path
            meaning. To make sure the nodes aren't a part of the core path, we check their children to make sure that
            detatching wont leave the child orphaned. If it will, we will preserve the edge.
            Note: I'm not certain that this is necessary, but it seems to work pretty well to preserve the paths. It
            might be worth extra experimentation later, but for MVP-sake, I think this is good.
            """
            for child_id in copy.deepcopy(node.children):
                child = data.get_node(child_id)
                if len(child.parents) > 1:
                    data.delete_edge(node_id, child_id)
            data.delete_node_and_preserve_edges(node_id)


def _merge_label_add_nodes_with_whenrules_nodes(data: GraphData) -> None:
    """
    Merge all label add nodes with their following WhenRules nodes
    """
    for node_id in data.get_node_ids():
        try:
            node = data.get_node(node_id)
        except IndexError:
            continue
        if node.type == NodeType.LABEL:
            new_parents: set[int] = set()
            new_children: dict[int, Any] = {}
            # Remove old edges and collect the edges of the WhenRules node
            for child_id in copy.deepcopy(node.children):
                child_node = data.get_node(child_id)
                if child_node.type != NodeType.RULE:
                    data.delete_edge(node_id, child_id)
                if child_node.type == NodeType.CALL and child_node.data['value'] == 'WhenRules':
                    new_children = copy.deepcopy(child_node.children)
                    new_children.pop(node_id, None)

                    new_parents_to_add = copy.deepcopy(child_node.parents)
                    new_parents_to_add.discard(node_id)
                    new_parents.update(new_parents_to_add)
            for parent_id in copy.deepcopy(node.parents):
                data.delete_edge(parent_id, node_id)
            # Apply the new edges from the WhenRules node
            for new_child_id in new_children:
                data.create_edge(node_id, new_child_id)
            for new_parent_id in new_parents:
                data.create_edge(new_parent_id, node_id)


def _recursively_collect_all_children_ids(data: GraphData, root_node: Node, node_ids: set[int]) -> None:
    """
    Iterates through the provided node's children and adds all of the IDs to node_ids
    """
    if root_node.id in node_ids:
        return
    node_ids.add(root_node.id)
    for child_id in root_node.children:
        _recursively_collect_all_children_ids(data, data.get_node(child_id), node_ids)


def _recursively_collect_all_parent_ids(data: GraphData, root_node: Node, node_ids: set[int]) -> None:
    """
    Iterates through the provided node's parents and adds all of the IDs to node_ids
    """
    if root_node.id in node_ids:
        return
    node_ids.add(root_node.id)
    for parent_id in root_node.parents:
        _recursively_collect_all_parent_ids(data, data.get_node(parent_id), node_ids)


def _create_label_view(
    data: GraphData,
    label_names: set[str],
    show_label_upstream: bool,
    show_label_downstream: bool,
) -> None:
    """
    Turns the provided GraphData into a label view graph by conforming to the upstream/downstream view requests
    """
    # Grab all nodes matching the requested label names
    root_nodes: set[Node] = set()
    safe_node_ids: set[int] = set()
    for node_id in data.get_node_ids():
        node = data.get_node(node_id)
        if node.type == NodeType.LABEL and node.data.get('label_name') in label_names:
            root_nodes.add(node)
            safe_node_ids.add(node_id)
    if show_label_upstream:
        # Collect all of the parent node ids for the label nodes
        for root_node in root_nodes:
            for parent_id in root_node.parents:
                _recursively_collect_all_parent_ids(data, data.get_node(parent_id), safe_node_ids)
    if show_label_downstream:
        # Collect all of the children node ids for the label nodes
        for root_node in root_nodes:
            for child_id in root_node.children:
                _recursively_collect_all_children_ids(data, data.get_node(child_id), safe_node_ids)
    # Delete all nodes that have no involvement with the requested label view
    for node_id in data.get_node_ids():
        if node_id not in safe_node_ids:
            data.delete_node(node_id)


def _graph_data_post_processing(
    data: GraphData,
    graph_view_type: GraphViewType,
    label_names: set[str],
    show_label_upstream: bool,
    show_label_downstream: bool,
) -> None:
    """
    Final post-processing; This step also applies any label filters to conform the graph to label views
    """
    for node_id in data.get_node_ids():
        node = data.get_node(node_id)
        if node.type in [NodeType.CALL]:
            data.delete_node_and_preserve_edges(node_id)
        elif node.type in [NodeType.FILE]:
            data.delete_node(node_id)
    if graph_view_type == GraphViewType.LABELS_VIEW:
        _create_label_view(data, label_names, show_label_upstream, show_label_downstream)


def process_graph_data(
    data: GraphData,
    graph_view_type: GraphViewType,
    label_names: set[str],
    show_label_upstream: bool,
    show_label_downstream: bool,
) -> None:
    """
    Applys any necessary filtering to compact the graph to a more human-readable format!
    """
    _graph_data_pre_processing(data)
    _merge_label_add_nodes_with_whenrules_nodes(data)
    _graph_data_post_processing(data, graph_view_type, label_names, show_label_upstream, show_label_downstream)


def render_graph(
    execution_graph: ExecutionGraph,  # The compiled execution graph to render
    action_names: set[str] | None = None,  # Action names to show
    label_names: set[str] | None = None,  # Label names to show
    show_label_upstream: bool = False,  # Upstream for label view
    show_label_downstream: bool = True,  # Downstream for label view
) -> 'RenderedDigraph':
    """
    Generate a rules vizualization graph based on the provided parameters.
    This method makes a call to the OspreyEngine Singleton to grab all valid label and action names.
    """
    if action_names is None:
        action_names = set()
    if label_names is None:
        label_names = set()

    all_action_names = ENGINE.instance().get_known_action_names()
    all_label_names = {key for key in ENGINE.instance().get_config_subkey(LabelsConfig).labels}

    return _render_graph(
        all_action_names,
        all_label_names,
        execution_graph,
        action_names,
        label_names,
        show_label_upstream,
        show_label_downstream,
    )


def _render_graph(
    all_action_names: set[str],  # All action names that exist within Osprey
    all_label_names: set[str],  # All label names that exist within Osprey Rules
    execution_graph: ExecutionGraph,  # The compiled execution graph to render
    action_names_to_render: set[str] | None = None,  # Action names to show
    label_names_to_render: set[str] | None = None,  # Label names to show
    show_label_upstream: bool = False,  # Upstream for label view
    show_label_downstream: bool = True,  # Downstream for label view
) -> 'RenderedDigraph':
    """
    A more testable helper method for the above render_graph() method.
    See: test_render_graph.py
    """
    if action_names_to_render is None:
        action_names_to_render = set()
    if label_names_to_render is None:
        label_names_to_render = set()

    if not action_names_to_render and not label_names_to_render:
        raise Exception('No actions or labels were specified. The graph could not be rendered.')

    # When generating label view, we want to generate all action names first (to find where the labels are)
    graph_view_type: GraphViewType
    if len(label_names_to_render) > 0:
        graph_view_type = GraphViewType.LABELS_VIEW
    else:
        graph_view_type = GraphViewType.ACTIONS_VIEW

    # Set this to false if you don't want label view to display unused labels
    show_unused_labels: bool = len(label_names_to_render) > 0

    data: GraphData = GraphData()

    sources = execution_graph._validated_sources.sources

    def associate_edge(source_id: int, target_id: int, color: str = '#4E5255') -> None:
        data.create_edge(source_id, target_id, {'color': color})

    def get_node_name(node: ASTNode, node_type: NodeType) -> str:
        name: str
        if isinstance(node, Call):
            if node_type == NodeType.FILE:
                name = f"{node_type.name}('{get_path_from_require_node(node)}')"
            else:
                name = f'{node_type.name}: {node.func.identifier}'
        elif isinstance(node, Literal) and isinstance(node, (String, Number, Boolean)):
            name = f'{node_type.name}({node.value!r})'
        elif isinstance(node, FormatString):
            name = f'{node_type.name}({node.format_string!r})'
        elif isinstance(node, BooleanOperation):
            name = f'{node_type.name}({node.operand.__class__.__name__})'
        elif isinstance(node, BinaryOperation):
            name = f'{node_type.name}({node.operator.__class__.__name__})'
        elif isinstance(node, BinaryComparison):
            name = f'{node_type.name}({node.comparator.__class__.__name__})'
        elif isinstance(node, Assign):
            if node_type == NodeType.RULE:
                name = 'Rule: ' + node.target.identifier_key
            else:
                name = node.target.identifier_key
        else:
            name = node_type.name

        if debug:
            path = get_node_path(node, node_type)
            name += ' [Type: ' + node_type.name + '] [Path: ' + path + ']'
        return name

    def get_node_value(node: ASTNode) -> Any | None:
        value: Any | None
        if isinstance(node, Call):
            path = get_path_from_require_node(node)
            if path:
                value = path
            else:
                value = f'{node.func.identifier}'
        elif isinstance(node, Literal) and isinstance(node, (String, Boolean, Number)):
            value = node.value
        elif isinstance(node, FormatString):
            value = node.format_string
        elif isinstance(node, BooleanOperation):
            value = f'{node.operand.__class__.__name__}'
        elif isinstance(node, BinaryOperation):
            value = f'{node.operator.__class__.__name__}'
        elif isinstance(node, BinaryComparison):
            value = f'{node.comparator.__class__.__name__}'
        elif isinstance(node, Assign):
            value = node.target.identifier_key
        else:
            value = None
        return value

    def get_node_type(node: ASTNode, node_type: Type[ASTNode], node_value: str | None) -> NodeType:
        if isinstance(node, Call) and node.func.identifier == 'Require' and node_value and node_value.endswith('.sml'):
            return NodeType.FILE
        elif is_rule_node(node):
            return NodeType.RULE
        return NodeType(node_type.__name__)

    def get_node_path(node: ASTNode, node_type: NodeType) -> str:
        """
        Returns the file path for this node
        """
        # If it's a file node (aka a "Requires" statement, for example), we will set the value as the file path.
        # Otherwise, the file path will be set to the file that this require code is in, and not the actual path that
        # is being required.
        if node_type == NodeType.FILE:
            value = get_node_value(node)
            if value:
                assert isinstance(value, str)
                return value
        return node.span.source.path

    def create_node_from_ast(node: ASTNode, node_type: Type[ASTNode]) -> Node:
        label_node = handle_label_node_if_present(node)
        if label_node:
            return label_node

        value = get_node_value(node)
        type = get_node_type(node, node_type, value)
        return data.create_node(
            name=get_node_name(node, type),
            type=type,
            file_path=get_node_path(node, type),
            value=value,
            span=str(node.span),
        )

    traversed_paths: set[str] = set()

    def handle_label_node_if_present(node: ASTNode) -> Node | None:
        if not isinstance(node, Call) or node.func.identifier not in [label.value for label in LabelType]:
            return None
        label_name: str = [i.value.value for i in node.arguments if i.name == 'label'][0]  # type: ignore
        entity_name: str = [i.value.identifier for i in node.arguments if i.name == 'entity'][0]  # type: ignore
        return create_node_from_label_name(
            label_name,
            entity_name,
            LabelType(node.func.identifier),
            get_node_path(node, NodeType.LABEL),
        )

    def handle_require_node_if_present(current_node: Node) -> None:
        """
        Adding a file to the execution graph (the main dependency chain does not normally go to other files)
        e.g. Require(rule=f'actions/{ActionName}.sml') -> File('actions/virus_uploaded.sml')
        """
        if current_node.type == NodeType.FILE:
            load_path(current_node.data['file_path'], current_node)

    referenced_label_nodes: set[str] = set()

    def create_node_from_label_name(
        label_name: str,
        entity_name: str | None,
        label_type: LabelType | None = None,
        path: str = 'Unknown path',
    ) -> Node | None:
        if label_name not in referenced_label_nodes:
            referenced_label_nodes.add(label_name)

        if entity_name:
            name = f"{label_type or 'Label'}: '{label_name}' on entity '{entity_name}'"
        else:
            name = f'Unassigned label {label_name}'

        node = data.create_node(
            name=name,
            type=NodeType.LABEL,
            file_path=path,
            value=(label_name, entity_name),
            extra_parameters={'label_name': label_name, 'entity_name': entity_name, 'label_type': label_type},
        )
        if debug:
            name += ' [Label Type: ' + str(label_type) + '] [Path: ' + path + ']'
        return node

    def load_path(
        path: str,
        current_node: Node,
        # If the path is formed from a variable (For example, virus_uploaded.sml from {ActionName}.sml)
        variable_path: bool = False,
    ) -> None:
        """
        Given a path to a .sml file, creates a tree from it and attaches it to the current_node
        """
        if not path.endswith('.sml'):
            return
        if not variable_path and '{ActionName}' in path:
            for action_name in all_action_names:
                current_path = path.replace('{ActionName}', action_name)
                if graph_view_type == GraphViewType.ACTIONS_VIEW and action_name not in action_names_to_render:
                    continue
                load_path(current_path, current_node, variable_path=True)
            return
        source = sources.get_by_path(path)
        if source and source.ast_root.statements:
            if path in traversed_paths:
                return
            traversed_paths.add(path)
            print_debug('recursing to ' + str(path))
            root_dependency: Node
            if variable_path:
                root_dependency = data.create_node(
                    name="File('" + path + "')",
                    type=NodeType.FILE,
                    file_path=path,
                    value=path,
                )
                associate_edge(current_node.id, root_dependency.id, '#FF0000')
            else:
                root_dependency = current_node
            for statement in source.ast_root.statements:
                chain = execution_graph.get_dependency_chain(statement)
                render_dependency_recursively(chain, root_dependency=root_dependency)

    def get_path_from_require_node(node: ASTNode) -> str | None:
        if not (isinstance(node, Call) and node.func.identifier == 'Require' and isinstance(node.func.context, Load)):
            return None
        encapsulated_string: str
        if node.arguments:
            if isinstance(node.arguments[0].value, FormatString):
                encapsulated_string = str(node.arguments[0].value.format_string)
            elif isinstance(node.arguments[0].value, String):
                encapsulated_string = str(node.arguments[0].value.value)
        if not encapsulated_string:
            print_debug('Could not find path for valid Require node: ' + str(node))
        return encapsulated_string

    def is_rule_node(node: ASTNode) -> bool:
        if isinstance(node, Assign) and isinstance(node.value, Call):
            if node.value.func.identifier == 'Rule':
                return True
        return False

    def render_dependency_recursively(
        current_dependency: DependencyChain,
        # A representation of the root node for the current dependency chain, if any.
        root_dependency: DependencyChain | Node | None = None,
        # Set this to skip the current node
        dependent_node: Node | None = None,
    ) -> Node:
        skip_next_node: bool = False
        if not dependent_node:
            node = create_node_from_ast(current_dependency.executor.node, current_dependency.executor.node_type)
            print_debug(str(node.id) + ': ' + str(current_dependency.executor.node))
            handle_require_node_if_present(node)
            if is_rule_node(current_dependency.executor.node) or node.type == NodeType.LABEL:
                skip_next_node = True
        else:
            node = dependent_node

        for dependency in current_dependency.dependent_on:
            if not skip_next_node:
                dependency_node = render_dependency_recursively(dependency, root_dependency=root_dependency)
                associate_edge(dependency_node.id, node.id, '#0000FF')
            else:
                render_dependency_recursively(dependency, root_dependency=root_dependency, dependent_node=node)

        if root_dependency and len(current_dependency.dependent_on) == 0:
            root_node: ASTNode | Node
            if isinstance(root_dependency, DependencyChain):
                root_node = create_node_from_ast(root_dependency.executor.node, root_dependency.executor.node_type)
                associate_edge(root_node.id, node.id, '#FF00FF')
            else:
                root_node = root_dependency
                assert isinstance(root_node, Node)
                associate_edge(root_node.id, node.id, '#FF0000')

        return node

    root_node = data.create_node(
        name="File('main.sml')",
        type=NodeType.FILE,
        value='main.sml',
        file_path='main.sml',
    )
    entry_point = execution_graph.get_entry_point()
    for statement in entry_point.ast_root.statements:
        render_dependency_recursively(
            execution_graph.get_dependency_chain(statement),
            root_node,
        )

    if show_unused_labels:
        for label in all_label_names:
            if label not in referenced_label_nodes:
                create_node_from_label_name(label, None)

    process_graph_data(data, graph_view_type, label_names_to_render, show_label_upstream, show_label_downstream)
    return RenderedDigraph(data.to_dict())


class RenderedDigraph:
    def _render_digraph_from_json_data(self, data: dict[str, Any]) -> Digraph:
        """
        Converts JSON data to a Digraph
        """
        dot = Digraph()
        dot.attr(rankdir='TB')
        dot.attr(bgcolor='#2C2F33')
        dot.attr(margin='0,0')
        dot.attr(pad='0.5,0.5')

        def get_attrs_from_json_node(node: dict[str, Any]) -> dict[str, str]:
            attrs = {
                'label': node['name'],
                'shape': 'box',
                'style': 'filled',
                'margin': '0.2,0.2',
                'color': '#404548',
                'fillcolor': '#23272A',
                'fontcolor': '#99AAB5',
                'fontname': 'sans-serif',
                'type': node['type'],
            }
            if node['type'] == 'Label':
                attrs['style'] = 'filled'
                attrs['fillcolor'] = '#ffd1dc'
                attrs['fontcolor'] = '#23272A'
            elif node['type'] in ['Literal', 'String', 'Number', 'Boolean']:
                attrs['shape'] = 'ellipse'
                attrs['margin'] = '0.05,0.05'
                attrs['fillcolor'] = '#404548'
                attrs['color'] = '#23272A'
                attrs['fontsize'] = '12'
            elif node['type'] == 'FormatString':
                attrs['shape'] = 'ellipse'
                attrs['margin'] = '0.05,0.05'
                attrs['fillcolor'] = '#404548'
                attrs['color'] = '#23272A'
                attrs['fontsize'] = '12'
            elif node['type'] == 'BooleanOperation':
                attrs['shape'] = 'invtrapezium'
                attrs['margin'] = '0.3,0'
            elif node['type'] == 'BinaryOperation':
                attrs['shape'] = 'invtrapezium'
                attrs['margin'] = '0.3,0'
            elif node['type'] == 'BinaryComparison':
                attrs['shape'] = 'invtrapezium'
                attrs['margin'] = '0.3,0'
            elif node['type'] == 'Assign' or node['type'] == 'File':
                attrs['shape'] = 'invhouse'
                attrs['style'] = 'filled'
                attrs['margin'] = '0.3,0.1'
                attrs['fillcolor'] = '#99AAB5'
                attrs['fontcolor'] = '#23272A'
            return attrs

        def get_edge_attrs(color: str) -> dict[str, str]:
            edge_attrs = {
                'color': color,
                'fontcolor': '#99AAB5',
                'fontsize': '10',
                'fontname': 'sans-serif',
                'arrowsize': '0.8',
                'labelfloat': 'true',
                'labeldistance': '0',
            }
            return edge_attrs

        for node in data['nodes']:
            dot.node(node['id'], None, get_attrs_from_json_node(node))
        for edge in data['edges']:
            dot.edge(edge['source'], edge['target'], _attributes=get_edge_attrs(edge['color']))
        return dot

    def __init__(self, data: dict[str, Any]):
        self._digraph: Digraph | None = None
        self.data = data

    def view(self) -> None:
        """Renders the graph and opens it for viewing in the default system photo viewer."""
        filename = tempfile.mkstemp(suffix='.gv')[1]
        if not self._digraph:
            self._digraph = self._render_digraph_from_json_data(self.data)
        self._digraph.render(filename=filename, cleanup=True, view=True)

    def render_to_file(self, path: pathlib.Path, format: str = 'pdf') -> None:
        """Renders the graph to a file with the given `path`, and `format`."""
        if not self._digraph:
            self._digraph = self._render_digraph_from_json_data(self.data)
        self._digraph.render(filename=str(path), format=format)

    def source(self) -> str:
        if not self._digraph:
            self._digraph = self._render_digraph_from_json_data(self.data)
        return self._digraph.source
