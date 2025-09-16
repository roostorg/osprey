import { memo, MutableRefObject, ReactElement, useEffect, useRef } from 'react';
import * as ReactDOM from 'react-dom/client';
import cytoscape, { Css, NodeSingular } from 'cytoscape';
import dagre, { DagreLayoutOptions } from 'cytoscape-dagre';
import popper from 'cytoscape-popper';
import tippy from 'tippy.js';

import { HierarchicalGraphOptions } from '../../types/RulesVisualizerTypes';

import 'tippy.js/dist/tippy.css';
import styles from './HierarchicalGraph.module.css';

const defaultNodeStyle: Css.Node = {
  'text-wrap': 'wrap',
  'text-valign': 'bottom',
  color: '#3c3c40',
};

const defaultEdgeStyle: Css.Edge = {
  width: 1,
  'target-arrow-shape': 'triangle',
  'line-color': '#9dbaea',
  'target-arrow-color': '#9dbaea',
  'curve-style': 'bezier',
};

const defaultlayoutOptions: DagreLayoutOptions = {
  name: 'dagre',
  fit: true,
  avoidOverlap: true,
  nodeDimensionsIncludeLabels: true,
  rankDir: 'LR',
};

cytoscape.use(dagre);
cytoscape.use(popper);

const HierarchicalGraph = ({
  elements,
  nodeStyle = {},
  edgeStyle = {},
  layoutOptions = {},
  onLoad = () => {},
  ToolTip,
}: HierarchicalGraphOptions) => {
  const containerRef = useRef(null);
  const toolTipRef = useRef(null);
  const tooltipRootRef = useRef<ReactDOM.Root | null>(null);

  useEffect(() => {
    const cy = cytoscape({
      container: containerRef.current,
      elements,
      style: [
        {
          selector: 'node',
          style: { ...defaultNodeStyle, ...nodeStyle },
        },
        {
          selector: 'edge',
          style: { ...defaultEdgeStyle, ...edgeStyle },
        },
      ],
      layout: { ...defaultlayoutOptions, ...layoutOptions },
      maxZoom: 10,
      autoungrabify: true,
    });
    onLoad(cy);

    let tip: any;
    if (ToolTip) {
      cy.nodes().bind('mouseover', (event) => {
        tip = renderToolTipWithTippy(event.target as NodeSingular, ToolTip, containerRef, toolTipRef, tooltipRootRef);
      });
      cy.nodes().bind('mouseout', () => {
        if (tip) {
          tip.destroy();
          tooltipRootRef.current?.unmount();
          tooltipRootRef.current = null;
        }
      });
    }

    return () => {
      if (tip) {
        tip.destroy();
        tooltipRootRef.current?.unmount();
        tooltipRootRef.current = null;
      }
      cy.nodes().unbind('mouseover');
      cy.nodes().unbind('mouseout');
      cy.destroy();
    };
  }, [elements, nodeStyle, edgeStyle, layoutOptions, onLoad, ToolTip]);

  return (
    <>
      <div ref={containerRef} className={styles.cyContainer} />
      <div ref={toolTipRef} />
    </>
  );
};

function renderToolTipWithTippy(
  node: NodeSingular,
  ToolTip: ({ node }: { node: NodeSingular }) => ReactElement,
  containerRef: MutableRefObject<null>,
  toolTipRef: MutableRefObject<null>,
  tooltipRootRef: MutableRefObject<ReactDOM.Root | null>
) {
  const popperRef = node.popperRef();
  if (toolTipRef.current) {
    tooltipRootRef.current = ReactDOM.createRoot(toolTipRef.current);
    tooltipRootRef.current.render(<ToolTip node={node} />);
  }
  if (containerRef.current && toolTipRef.current) {
    const tip: any = tippy(containerRef.current, {
      getReferenceClientRect: popperRef.getBoundingClientRect,
      content: toolTipRef.current,
      placement: 'bottom',
      arrow: true,
    });
    tip.show();
    return tip;
  }
}

export default memo(HierarchicalGraph, (prevProps, nextProps) => {
  return (
    JSON.stringify(prevProps.elements) == JSON.stringify(nextProps.elements) &&
    prevProps.nodeStyle == nextProps.nodeStyle &&
    prevProps.edgeStyle == nextProps.edgeStyle &&
    prevProps.layoutOptions == nextProps.layoutOptions &&
    prevProps.ToolTip == nextProps.ToolTip
  );
});
