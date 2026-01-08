from datetime import datetime
from typing import Any

from flask import Blueprint, jsonify
from osprey.engine.executor.execution_visualizer import RenderedDigraph, render_graph
from osprey.worker.lib.singletons import ENGINE
from osprey.worker.sinks.sink.output_sink_utils.models import OspreyRulesVisualizerGenGraphAnalyticsEvent
from osprey.worker.ui_api.osprey.lib.marshal import JsonBodyMarshaller, marshal_with
from pydantic.main import BaseModel

from ..singletons import ANALYTICS_PUBLISHER

blueprint = Blueprint('rules_visualizer', __name__)


class BaseActionsViewQuery(BaseModel, JsonBodyMarshaller):
    action_names: list[str]


class BaseLabelsViewQuery(BaseModel, JsonBodyMarshaller):
    label_names: list[str]
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
    ANALYTICS_PUBLISHER.instance().publish(
        OspreyRulesVisualizerGenGraphAnalyticsEvent(
            path='/rules_visualizer/actions_view/',
            request_method='POST',
            source_features=query.action_names,
            timestamp=datetime.now().isoformat(),
        )
    )
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

    params = f'{"downstream/" if query.show_downstream else ""}{"upstream/" if query.show_upstream else ""}'
    path = f'/rules_visualizer/labels_view/{params}'
    ANALYTICS_PUBLISHER.instance().publish(
        OspreyRulesVisualizerGenGraphAnalyticsEvent(
            path=path, request_method='POST', source_features=query.label_names, timestamp=datetime.now().isoformat()
        )
    )
    return jsonify(graph.data)
