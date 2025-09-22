from typing import Any, List

from flask import Blueprint, jsonify
from osprey.engine.executor.execution_visualizer import RenderedDigraph, render_graph
from osprey.worker.lib.singletons import ENGINE
from osprey.worker.ui_api.osprey.lib.marshal import JsonBodyMarshaller, marshal_with
from pydantic.main import BaseModel

blueprint = Blueprint('rules_visualizer', __name__)


class BaseActionsViewQuery(BaseModel, JsonBodyMarshaller):
    action_names: List[str]


class BaseLabelsViewQuery(BaseModel, JsonBodyMarshaller):
    label_names: List[str]
    show_upstream: bool = False
    show_downstream: bool = True


@blueprint.route('/rules_visualizer/actions_view/', methods=['POST'])
@marshal_with(BaseActionsViewQuery)
def get_rules_visualizer_by_action_names(query: BaseActionsViewQuery) -> Any:
    """
    Generate an action view ruleviz graph.
    Returns {
        nodes: [
            id: ...
            ... (node data)
        ],
        edges: [
            source: ...
            target: ...
            ... (edge data)
        ]
    }
    """
    engine = ENGINE.instance()
    graph: RenderedDigraph = render_graph(engine.execution_graph, action_names=set(query.action_names))
    return jsonify(graph.data)


@blueprint.route('/rules_visualizer/labels_view/', methods=['POST'])
@marshal_with(BaseLabelsViewQuery)
def get_rules_visualizer_by_label_names(query: BaseLabelsViewQuery) -> Any:
    """
    Generate a label view ruleviz graph.
    Returns {
        nodes: [
            id: ...
            ... (node data)
        ],
        edges: [
            source: ...
            target: ...
            ... (edge data)
        ]
    }
    """
    engine = ENGINE.instance()
    graph: RenderedDigraph = render_graph(
        engine.execution_graph,
        label_names=set(query.label_names),
        show_label_upstream=query.show_upstream,
        show_label_downstream=query.show_downstream,
    )
    return jsonify(graph.data)
