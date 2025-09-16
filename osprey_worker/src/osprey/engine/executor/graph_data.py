import copy
from enum import StrEnum
from typing import Any, Dict, List, Optional, Set, Tuple

from typing_extensions import TypedDict


class NodeType(StrEnum):
    UNASSIGNED = 'Unassigned'
    FILE = 'File'
    RULE = 'Rule'
    LABEL = 'Label'
    LIST = 'List'
    LITERAL = 'Literal'
    NUMBER = 'Number'
    BOOLEAN = 'Boolean'
    STRING = 'String'
    FORMAT_STRING = 'FormatString'
    NONE_ = 'None_'
    BOOLEAN_OPERATION = 'BooleanOperation'
    UNARY_OPERATION = 'UnaryOperation'
    BINARY_COMPARISON = 'BinaryComparison'
    BINARY_OPERATION = 'BinaryOperation'
    ASSIGN = 'Assign'
    CALL = 'Call'
    IMPORT = 'Import'


class LabelType(StrEnum):
    ADD = 'LabelAdd'
    REMOVE = 'LabelRemove'
    CHECK = 'HasLabel'


class NodeData(TypedDict, total=False):
    id: int
    name: str
    type: NodeType
    file_path: str
    value: Optional[Any]
    num_children: Optional[int]
    label_name: Optional[str]
    label_type: Optional[LabelType]
    entity_name: Optional[str]


GRAPH_VIZ_NODE_TYPES = {NodeType.LABEL, NodeType.RULE}


class Node:
    def __init__(self, id: int, name: str, type: NodeType, file_path: str, value: Optional[Any]) -> None:
        """
        Data structure:

        {
            "id": 1,
            "data": {
                "name": "A",
                ...
            },
            "parents": {2},
            "children": {
                3: {
                    "color": "#FFFFFF",
                    ... (other data)
                },
            }
        }
        """
        self.id: int = id
        self.children: Dict[int, Any] = {}
        self.parents: Set[int] = set()
        self.data: NodeData = {'id': id, 'name': name, 'type': type, 'file_path': file_path, 'value': value}
        self.type: NodeType = type

    def put_values(self, values: NodeData) -> None:
        """
        Puts extra optional values in the values dict into this node
        """
        self.data.update(values)

    def is_empty(self) -> bool:
        return self.type == NodeType.UNASSIGNED


class GraphData:
    def __init__(self) -> None:
        """
        Data structure:

        {
            1: {
                "id": 1,
                "name": "A",
                ...
                "parents": {2},
                "children": {
                    3: {
                        "color": "#FFFFFF",
                        ...
                    },
                }
            },
            2: {
                "id": 2,
                "name": "B",
                ...
                "parents": set(),
                "children": {
                    1: {
                        "color": "#FFFFFF",
                        ...
                    },
                }
            },
            3: {
                "id": 3,
                "name": "C",
                ...
                "parents": {1},
                "children": {},
            },
            ...
        }
        Why?
        O(1) indexing for nodes by ID; O(1) indexing for edges by ID;
        O(1) + O(P+C) where P+C is the number of parents + children for node insertion and deletion;
        O(1) insertion and deletion for edges;
        Allows us to store edge data
        """
        self._data: Dict[int, Node] = {}
        self._auto_increment: int = 1
        # Key: Hash of contents, Value: ID of first node found with those contents
        self._created_node_id_by_contents: Dict[int, int] = {}
        self._created_edges: Set[Tuple[int, int]] = set()

    def get_node(self, id: int) -> Node:
        """
        Get a node from the graph by ID
        """
        node: Optional[Node] = self._data.get(id)
        if not node:
            raise IndexError
        return node

    def get_node_ids(self) -> Set[int]:
        node_ids = {k for k in self._data}
        return node_ids

    def get_edges(self) -> Set[Tuple[int, int]]:
        edges: Set[Tuple[int, int]] = set()
        for node_id in self.get_node_ids():
            node = self.get_node(node_id)
            for child_id in node.children:
                edges.add((node_id, child_id))
        return copy.deepcopy(edges)

    def _get_parents_with_data(self, id: int) -> Dict[int, Any]:
        """
        Get all parents for the given node (including the edge data)
        """
        parents: Set[int] = self.get_node(id).parents
        parents_with_data: Dict[int, Any] = {}
        for parent in parents:
            try:
                parents_with_data[parent] = self.get_node(parent).children[id]
            except IndexError:
                print('Invalid node id: ' + str(parent))
                continue
            except KeyError:
                print('Invalid node parent id: ' + str(id))
                continue
        return parents_with_data

    def create_node(
        self,
        name: str,
        type: NodeType,
        file_path: str,
        value: Optional[Any],
        extra_parameters: Optional[NodeData] = None,
        span: Optional[str] = None,
    ) -> Node:
        """
        Creates a new node at the given ID.
        """
        if extra_parameters is None:
            extra_parameters = {}
        contents_hash = self._get_contents_hash(type, value, span, extra_parameters)
        if contents_hash in self._created_node_id_by_contents:
            return self.get_node(self._created_node_id_by_contents[contents_hash])

        new_id = self._auto_increment
        self._auto_increment += 1
        node = Node(new_id, name, type, file_path, value)
        if extra_parameters:
            node.put_values(extra_parameters)
        self._created_node_id_by_contents[contents_hash] = new_id
        self._data[new_id] = node
        return node

    def _get_contents_hash(
        self,
        type: NodeType,
        value: Optional[Any],
        span: Optional[str] = None,
        extra_parameters: Optional[NodeData] = None,
    ) -> int:
        if extra_parameters is None:
            extra_parameters = {}
        if type in GRAPH_VIZ_NODE_TYPES:
            return hash(
                (
                    type,
                    value,
                    extra_parameters.get('label_name'),
                    extra_parameters.get('label_type'),
                    extra_parameters.get('entity_name'),
                )
            )
        else:
            return hash((type, value, span))

    def create_edge(self, source_id: int, target_id: int, edge_data: Optional[Dict[str, Any]] = None) -> None:
        """
        Attach the provided source node to the provided target node
        """
        if edge_data is None:
            edge_data = {'color': '#E6E6E6'}

        edge = (source_id, target_id)
        if edge in self._created_edges:
            return

        children = self._data[source_id].children
        if not children.get(target_id):
            children[target_id] = edge_data
        self._data[target_id].parents.add(source_id)
        self._created_edges.add(edge)

    def delete_edge(self, source_id: int, target_id: int) -> None:
        """
        Deletes an edge from the graph by IDs
        """
        try:
            parent_node = self.get_node(source_id)
            del parent_node.children[target_id]
        except (KeyError, IndexError):
            print('Unexpected edge from ' + str(source_id) + ' to ' + str(target_id))
        try:
            self.get_node(target_id).parents.remove(source_id)
        except (KeyError, IndexError):
            print('Unexpected edge from ' + str(source_id) + ' to ' + str(target_id))

    def delete_node(self, id: int) -> Any:
        """
        Deletes a node from the graph by ID
        """
        node = self.get_node(id)
        for parent_id in copy.deepcopy(node.parents):
            self.delete_edge(parent_id, id)
        for child_id in copy.deepcopy(node.children):
            self.delete_edge(id, child_id)
        del self._data[id]

    def delete_node_and_preserve_edges(self, id: int) -> Any:
        """
        Deletes a node from the graph by ID;
        This method also reattaches the parents of the node to it's children (to compact the graph)
        """

        def get_average_hex(color_a: Optional[str], color_b: Optional[str]) -> Optional[str]:
            """
            Gets the average of two HEX colors
            """
            if not color_a and not color_b:
                return None
            if not color_a:
                return color_b
            if not color_b:
                return color_a
            color_a_decimal = int(color_a.lstrip('#'), 16)
            color_b_decimal = int(color_b.lstrip('#'), 16)
            color_c_decimal = round((color_a_decimal + color_b_decimal) / 2)
            return '#{0:06X}'.format(color_c_decimal).lstrip('0x').rstrip('L')

        parents: Dict[int, Any] = self._get_parents_with_data(id)
        children: Dict[int, Any] = copy.deepcopy(self._data[id].children)
        for child_id in children:
            self.delete_edge(id, child_id)
        for parent_id in parents:
            self.delete_edge(parent_id, id)
            for child_id in children:
                if child_id == parent_id:
                    continue
                new_color = get_average_hex(parents[parent_id].get('color'), children[child_id].get('color'))
                if new_color:
                    self.create_edge(parent_id, child_id, {'color': new_color})
                else:
                    self.create_edge(parent_id, child_id)
        del self._data[id]

    def _get_num_children(self, id: int) -> int:
        """
        Recursively counts the number of children below this node
        """

        def get_num_children_recursively(id: int, visited_ids: Set[int]) -> int:
            children = 0
            if id in visited_ids:
                return children
            visited_ids.add(id)
            current_node_children = self.get_node(id).children
            for child in current_node_children:
                children += 1 + get_num_children_recursively(child, visited_ids)
            return children

        return get_num_children_recursively(id, set())

    def get_node_ids_by_num_children(self) -> List[int]:
        """
        Returns a list of node IDs sorted in descending order from most-children to least-children
        """
        for node_id, node in self._data.items():
            num_children = self._get_num_children(node_id)
            node.put_values({'num_children': num_children})

        return list(
            reversed(
                [
                    item[0]
                    for item in sorted(self._data.items(), key=lambda kv: kv[1].data['num_children'])  # type: ignore
                ]
            )
        )

    def to_dict(self) -> Dict[str, Any]:
        output_dict: Dict[str, Any] = {'nodes': [], 'edges': []}
        node_ids = self.get_node_ids_by_num_children()
        for node_id in node_ids:
            node = self.get_node(node_id)
            if node.is_empty():
                print(f'Node {node_id} was empty! Removing...')
                continue
            output_dict['nodes'].append(node.data)
            for child_id, child_data in node.children.items():
                edge_data = {
                    'source': node_id,
                    'target': child_id,
                }
                for key, val in child_data.items():
                    edge_data[key] = val
                output_dict['edges'].append(edge_data)
        self._clean_edges(output_dict['edges'], output_dict['nodes'])
        return output_dict

    def _clean_edges(self, edges: List[Dict[str, Any]], nodes: List[Dict[str, Any]]) -> None:
        for i in range(len(edges) - 1, -1, -1):
            edge_data = edges[i]
            missing_source = True
            missing_target = True
            for node in nodes:
                assert isinstance(node, dict)
                if node['id'] == edge_data['source']:
                    missing_source = False
                if node['id'] == edge_data['target']:
                    missing_target = False
            if missing_source or missing_target:
                del edges[i]
                print(f'Edge ({edge_data["source"]}, {edge_data["target"]}) had a missing node! Removing...')
