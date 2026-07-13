import HTTPUtils, { HTTPResponse } from '../utils/HTTPUtils';
import { Node, Edge } from '../types/RulesVisualizerTypes';

interface GraphJsonResponse {
  nodes: Node[] | null;
  edges: Edge[] | null;
  errorMessage?: string;
  selectedFeature?: string;
  selectedFeatureType?: 'Action' | 'Label';
}

export async function getGraphJson(
  type: string,
  names: string[],
  showLabelUpstream: boolean,
  showLabelDownstream: boolean
): Promise<GraphJsonResponse> {
  if (type === 'label') {
    return await getLabelsViewGraphJson(names, showLabelUpstream, showLabelDownstream);
  } else if (type === 'action') {
    return await getActionsViewGraphJson(names);
  }
  return {
    nodes: null,
    edges: null,
    errorMessage: 'Cannot formulate request: unrecognized selected feature type.',
    selectedFeature: names[0],
  };
}

async function getActionsViewGraphJson(action_names: Array<string>): Promise<GraphJsonResponse> {
  const response: HTTPResponse = await HTTPUtils.post(`/rules_visualizer/actions_view/`, { action_names });
  const selection = {
    selectedFeature: action_names[0],
    selectedFeatureType: 'Action' as const,
  };

  if (response.ok) {
    const data = response.data as unknown as { nodes?: Node[]; edges?: Edge[] };
    return {
      nodes: data.nodes ?? null,
      edges: data.edges ?? null,
      ...selection,
    };
  }
  return {
    nodes: null,
    edges: null,
    errorMessage: response.error.message,
    ...selection,
  };
}

async function getLabelsViewGraphJson(
  label_names: Array<string>,
  showLabelUpstream: boolean,
  showLabelDownstream: boolean
): Promise<GraphJsonResponse> {
  const response: HTTPResponse = await HTTPUtils.post(`/rules_visualizer/labels_view/`, {
    label_names,
    show_upstream: showLabelUpstream,
    show_downstream: showLabelDownstream,
  });
  const selection = {
    selectedFeature: label_names[0],
    selectedFeatureType: 'Label' as const,
  };

  if (response.ok) {
    const data = response.data as unknown as { nodes?: Node[]; edges?: Edge[] };
    return {
      nodes: data.nodes ?? null,
      edges: data.edges ?? null,
      ...selection,
    };
  }
  return {
    nodes: null,
    edges: null,
    errorMessage: response.error.message,
    ...selection,
  };
}
