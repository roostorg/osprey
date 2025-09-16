import HTTPUtils, { HTTPResponse } from '../utils/HTTPUtils';

export async function getGraphJson(
  type: string,
  names: string[],
  showLabelUpstream: boolean,
  showLabelDownstream: boolean
) {
  if (type === 'label') {
    return await getLabelsViewGraphJson(names, showLabelUpstream, showLabelDownstream);
  } else if (type === 'action') {
    return await getActionsViewGraphJson(names);
  }
  return {
    errorMessage: 'Cannot formulate request: unrecognized selected feature type.',
    selectedFeature: names[0],
  };
}

async function getActionsViewGraphJson(action_names: Array<string>): Promise<any> {
  const response: HTTPResponse = await HTTPUtils.post(`/rules_visualizer/actions_view/`, { action_names });
  const selection = {
    selectedFeature: action_names[0],
    selectedFeatureType: 'Action',
  };

  if (response.ok) {
    return {
      ...response.data,
      ...selection,
      errorMessage: undefined,
    };
  }
  return {
    errorMessage: response.error.message,
    ...selection,
  };
}

async function getLabelsViewGraphJson(
  label_names: Array<string>,
  showLabelUpstream: boolean,
  showLabelDownstream: boolean
): Promise<any> {
  const response: HTTPResponse = await HTTPUtils.post(`/rules_visualizer/labels_view/`, {
    label_names,
    show_upstream: showLabelUpstream,
    show_downstream: showLabelDownstream,
  });
  const selection = {
    selectedFeature: label_names[0],
    selectedFeatureType: 'Label',
  };

  if (response.ok) {
    return {
      ...response.data,
      ...selection,
      errorMessage: undefined,
    };
  }
  return {
    errorMessage: response.error.message,
    ...selection,
  };
}
