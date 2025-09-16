from datetime import datetime

from ..graph_data import GraphData, Node, NodeType


def create_node(data: GraphData) -> Node:
    unique_suffix = datetime.now()
    return data.create_node('Test name', NodeType.STRING, 'main.sml', f'This is the string! {unique_suffix}')


def test_node_creation() -> None:
    data = GraphData()
    node_id = data.create_node(
        'Test name', NodeType.STRING, 'main.sml', 'This is the string!', {'label_name': 'an extra param~'}
    ).id
    node = data.get_node(node_id)
    assert node.type == NodeType.STRING
    assert node.data['id'] == node.id
    assert node.data['type'] == node.type
    assert node.data['name'] == 'Test name'
    assert node.data['file_path'] == 'main.sml'
    assert node.data['value'] == 'This is the string!'
    assert node.data['name'] == 'Test name'
    assert node.data['label_name'] == 'an extra param~'


def test_node_deletion() -> None:
    data = GraphData()
    node = create_node(data)
    id = node.id
    data.delete_node(id)
    try:
        data.get_node(id)
    except IndexError:
        return
    assert False, 'Node deletion failed!'


def test_node_deletion_with_edge_preservation() -> None:
    """  # noqa: W605
    We are testing that the following graph:

    1-----2
     \   /
       3
     /   \
    4     5

    Becomes:

    1-----2
    | \ / |
    |  X  |
    | / \ |
    4     5

    After deleting node 3 with edge preservation
    """  # noqa: W605
    data = GraphData()
    for _ in range(0, 5):
        create_node(data)
    data.create_edge(1, 3)
    data.create_edge(1, 2)
    data.create_edge(2, 3)
    data.create_edge(3, 4)
    data.create_edge(3, 5)

    data.delete_node_and_preserve_edges(3)
    new_edges = data.get_edges()

    assert (1, 3) not in new_edges
    assert (2, 3) not in new_edges
    assert (3, 4) not in new_edges
    assert (3, 5) not in new_edges
    assert (1, 2) in new_edges
    assert (1, 5) in new_edges
    assert (1, 4) in new_edges
    assert (2, 4) in new_edges
    assert (2, 5) in new_edges
    assert len(new_edges) == 5


def test_node_deletion_without_edge_preservation() -> None:
    """  # noqa: W605
    We are testing that the following graph:

    1     2
    |\   /
    |  3
    |/   \
    4     5

    Becomes:

    1     2
    |
    4     5

    After deleting node 3 without edge preservation
    """
    data = GraphData()
    for _ in range(0, 5):
        create_node(data)
    data.create_edge(1, 3)
    data.create_edge(1, 4)
    data.create_edge(2, 3)
    data.create_edge(3, 4)
    data.create_edge(3, 5)

    data.delete_node(3)
    new_edges = data.get_edges()

    assert (1, 3) not in new_edges
    assert (2, 3) not in new_edges
    assert (3, 4) not in new_edges
    assert (3, 5) not in new_edges
    assert (1, 4) in new_edges
    assert len(new_edges) == 1
