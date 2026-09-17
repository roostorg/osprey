import { useState } from 'react';
import { Alert, Button, Card, Spin, Switch } from 'antd';
import { AimOutlined, ZoomInOutlined, ZoomOutOutlined } from '@ant-design/icons';
import shallow from 'zustand/shallow';
import { Css, Core } from 'cytoscape';

import useRulesVisualizerStore from '../../stores/RulesVisualizerStore';
import HierarchicalGraph from './HierarchicalGraph';
import RulesVisualizerHeader from './RulesVisualizerHeader';
import { Node, NodeType, LabelType } from '../../types/RulesVisualizerTypes';

import styles from './RulesVisualizer.module.css';
import { getGraphJson } from '../../actions/RulesVisualizerActions';

export const DEFAULT_ANIMATE_DURATION = 1000;
// Lets users zoom out to 30% the size of the initial whole-graph fit.
const MIN_ZOOM_FIT_MULTIPLIER = 0.3;
// Relative amount each +/- zoom control click changes the zoom level by.
const ZOOM_STEP_FACTOR = 1.2;

const typeToShape: Record<string, Css.NodeShape> = {
  [NodeType.Label]: 'ellipse',
  [NodeType.Rule]: 'round-rectangle',
};

const nodeStyle: Css.Node = {
  content: 'data(label)',
  'background-color': (node) => getNodeColor(node.data('type'), node.data('label_type')),
  shape: (node) => typeToShape[node.data('type')],
};

const ToolTip = ({ node }: { node: { data: (key: string) => unknown } }) => {
  return <div>{String(node.data('file_path'))}</div>;
};

const RulesVisualizerView = () => {
  const [updateRuleVizGraph, nodes, edges, selectedFeature, selectedFeatureType, errorMessage] =
    useRulesVisualizerStore(
      (state) => [
        state.updateRuleVizGraph,
        state.nodes,
        state.edges,
        state.selectedFeature,
        state.selectedFeatureType,
        state.errorMessage,
      ],
      shallow
    );
  const [isLoading, setIsLoading] = useState(false);
  const [showLabelUpstream, setShowLabelUpstream] = useState(false);
  const [showLabelDownstream, setShowLabelDownstream] = useState(true);
  const [cyto, setCyto] = useState<Core | null>(null);
  // baseZoom is the zoom level cytoscape picked to fit the whole graph on load. Displayed zoom
  // percentage is relative to this, so "100%" always means "the whole graph fits" regardless of
  // how large or small that fit zoom actually is in cytoscape's own units.
  const [baseZoom, setBaseZoom] = useState<number | null>(null);
  const [zoomLevel, setZoomLevel] = useState<number | null>(null);

  const elements = {
    nodes: (nodes || []).map((node) => ({
      data: {
        id: `${node.id}`,
        label: getLabel(node),
        type: node.type,
        label_type: node.label_type,
        label_name: node.label_name,
        entity_name: node.entity_name,
        file_path: node.file_path,
      },
    })),
    edges: (edges || []).map((edge, idx) => ({
      data: {
        id: `edge-${idx}`,
        source: `${edge.source}`,
        target: `${edge.target}`,
      },
    })),
  };

  let alert;
  if (nodes && !nodes.length) {
    alert = <Alert className={styles.centered} type="warning" message="No associated nodes were found." />;
  } else if (errorMessage) {
    alert = <Alert className={styles.centered} type="error" message={`Error: ${errorMessage}. Please try again.`} />;
  }

  const onGraphLoad = (cy: Core) => {
    // View defaults to fitting whole graph within viewport. Allow zooming out further
    // than that fit level so large graphs can be shrunk down more before panning is needed.
    cy.minZoom(cy.zoom() * MIN_ZOOM_FIT_MULTIPLIER);
    setBaseZoom(cy.zoom());
    setZoomLevel(cy.zoom());
    cy.on('zoom', () => setZoomLevel(cy.zoom()));
    setCyto(cy);
  };

  const recenterOnClick = () => {
    if (cyto) {
      cyto.animate({
        easing: 'ease-in-out',
        duration: DEFAULT_ANIMATE_DURATION,
        fit: { eles: cyto.elements(), padding: 0 },
      });
    }
  };

  const zoomByFactor = (factor: number) => {
    if (cyto) {
      cyto.zoom(cyto.zoom() * factor);
    }
  };

  const onShowLabelUpstreamToggle = async (checked: boolean) => {
    setShowLabelUpstream(checked);
    await rerenderLabelViewGraph(checked, showLabelDownstream);
  };

  const onShowLabelDownstreamToggle = async (checked: boolean) => {
    setShowLabelDownstream(checked);
    await rerenderLabelViewGraph(showLabelUpstream, checked);
  };

  const rerenderLabelViewGraph = async (show_upstream: boolean, show_downstream: boolean) => {
    if (!selectedFeature || !selectedFeatureType) {
      return;
    }
    setIsLoading(true);
    updateRuleVizGraph({ nodes: null, edges: null });
    const graphJson = await getGraphJson(
      selectedFeatureType.toLowerCase(),
      [`${selectedFeature}`],
      show_upstream,
      show_downstream
    );
    updateRuleVizGraph(graphJson);
    setIsLoading(false);
  };

  return (
    <div className={styles.viewContainer}>
      <RulesVisualizerHeader
        cy={cyto}
        setIsLoading={setIsLoading}
        showLabelUpstream={showLabelUpstream}
        showLabelDownstream={showLabelDownstream}
      />
      <Spin className={styles.centered} size="large" spinning={isLoading} />
      {alert}
      <div className={styles.graphContainer}>
        {selectedFeatureType === 'Label' && (
          <Card size="small" title="Label View Filters" className={styles.recenterCard}>
            <Switch onChange={onShowLabelUpstreamToggle} disabled={isLoading} defaultChecked={showLabelUpstream} /> Show
            Upstream Nodes
            <br></br>
            <Switch
              onChange={onShowLabelDownstreamToggle}
              disabled={isLoading}
              defaultChecked={showLabelDownstream}
            />{' '}
            Show Downstream Nodes
          </Card>
        )}
        <HierarchicalGraph elements={elements} nodeStyle={nodeStyle} onLoad={onGraphLoad} ToolTip={ToolTip} />
        {nodes && !!nodes.length && (
          <div className={styles.zoomControls}>
            <Button type="text" icon={<ZoomOutOutlined />} onClick={() => zoomByFactor(1 / ZOOM_STEP_FACTOR)} />
            <span className={styles.zoomLabel}>
              {zoomLevel !== null && baseZoom !== null ? `${Math.round((zoomLevel / baseZoom) * 100)}%` : '—'}
            </span>
            <Button type="text" icon={<ZoomInOutlined />} onClick={() => zoomByFactor(ZOOM_STEP_FACTOR)} />
            <span className={styles.zoomDivider} />
            <Button type="text" icon={<AimOutlined />} onClick={recenterOnClick} />
          </div>
        )}
      </div>
    </div>
  );
};

function getLabel(node: Node) {
  if (node.type === NodeType.Label) {
    return `${node.type} [${getLabelTypeName(node.label_type)}]\n${node.label_name}\non ${
      node.entity_name ? node.entity_name : 'Unassigned'
    }`;
  }
  return `${node.type}\n${node.value}`;
}

function getLabelTypeName(label_type?: string) {
  const labelTypeName = Object.entries(LabelType).find(([, value]) => value === label_type);
  return labelTypeName?.[0] || '';
}

function getNodeColor(type: string, label_type?: string) {
  if (type === NodeType.Label && label_type == LabelType.Check) {
    return '#DEA39E';
  } else if (type === NodeType.Label && label_type == LabelType.Add) {
    return '#D4DE9E';
  } else if (type === NodeType.Label && label_type == LabelType.Remove) {
    return '#BC916E';
  } else if (type === NodeType.Rule) {
    return '#CAE0F9';
  }
  return '#8F8F8F';
}

export default RulesVisualizerView;
