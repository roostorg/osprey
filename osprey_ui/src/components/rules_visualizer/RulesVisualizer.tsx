import { useCallback, useMemo, useState } from 'react';
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
import { GRAPH_FIT_PADDING, ZOOM_STEP_FACTOR, computeMinZoom, computeZoomPercentage } from './zoomUtils';

export const DEFAULT_ANIMATE_DURATION = 1000;

const typeToShape: Record<string, Css.NodeShape> = {
  [NodeType.Label]: 'ellipse',
  [NodeType.Rule]: 'round-rectangle',
};

const nodeStyle: Css.Node = {
  content: 'data(label)',
  'background-color': (node) => getNodeColor(node.data('type'), node.data('label_type')),
  shape: (node) => typeToShape[node.data('type')],
};

// Stable reference — HierarchicalGraph rebuilds the whole graph when this identity changes.
const layoutOptions = { padding: GRAPH_FIT_PADDING };

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
  // baseZoom is the fit zoom on load; displayed percentage is relative to it, so 100% = whole graph fits.
  const [baseZoom, setBaseZoom] = useState<number | null>(null);
  const [zoomLevel, setZoomLevel] = useState<number | null>(null);

  // Memoized so a re-render (e.g. the zoom display updating) doesn't rebuild the whole graph.
  const elements = useMemo(
    () => ({
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
    }),
    [nodes, edges]
  );

  let alert;
  if (nodes && !nodes.length) {
    alert = <Alert className={styles.centered} type="warning" message="No associated nodes were found." />;
  } else if (errorMessage) {
    alert = <Alert className={styles.centered} type="error" message={`Error: ${errorMessage}. Please try again.`} />;
  }

  const onGraphLoad = useCallback((cy: Core) => {
    cy.minZoom(computeMinZoom(cy.zoom()));
    setBaseZoom(cy.zoom());
    setZoomLevel(cy.zoom());
    cy.on('zoom', () => setZoomLevel(cy.zoom()));
    // cytoscape also fires 'resize' for unrelated attribute changes, so only react to real ones.
    let lastWidth = cy.width();
    let lastHeight = cy.height();
    cy.on('resize', () => {
      const width = cy.width();
      const height = cy.height();
      if (width === lastWidth && height === lastHeight) {
        return;
      }
      lastWidth = width;
      lastHeight = height;
      if (width <= 2 * GRAPH_FIT_PADDING || height <= 2 * GRAPH_FIT_PADDING) {
        return; // too small to fit the padding — fit zoom would be zero or negative
      }
      cy.minZoom(0); // lift the old floor so it can't clip the new fit
      cy.fit(cy.elements(), GRAPH_FIT_PADDING);
      const fitZoom = cy.zoom();
      cy.minZoom(computeMinZoom(fitZoom));
      setBaseZoom(fitZoom);
    });
    setCyto(cy);
  }, []);

  const recenterOnClick = () => {
    if (cyto) {
      cyto.animate({
        easing: 'ease-in-out',
        duration: DEFAULT_ANIMATE_DURATION,
        fit: { eles: cyto.elements(), padding: GRAPH_FIT_PADDING },
      });
    }
  };

  const zoomByFactor = (factor: number) => {
    if (cyto) {
      // Anchor on the viewport center; a bare number would zoom from the pan origin instead.
      cyto.zoom({
        level: cyto.zoom() * factor,
        renderedPosition: { x: cyto.width() / 2, y: cyto.height() / 2 },
      });
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
        <HierarchicalGraph
          elements={elements}
          nodeStyle={nodeStyle}
          layoutOptions={layoutOptions}
          onLoad={onGraphLoad}
          ToolTip={ToolTip}
        />
        {nodes && !!nodes.length && (
          <div className={styles.zoomControls}>
            <Button
              type="text"
              aria-label="Zoom out"
              icon={<ZoomOutOutlined />}
              onClick={() => zoomByFactor(1 / ZOOM_STEP_FACTOR)}
            />
            <span className={styles.zoomLabel}>
              {zoomLevel !== null && baseZoom !== null ? `${computeZoomPercentage(zoomLevel, baseZoom)}%` : '—'}
            </span>
            <Button
              type="text"
              aria-label="Zoom in"
              icon={<ZoomInOutlined />}
              onClick={() => zoomByFactor(ZOOM_STEP_FACTOR)}
            />
            <span className={styles.zoomDivider} />
            <Button type="text" aria-label="Recenter" icon={<AimOutlined />} onClick={recenterOnClick} />
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
