import { memo, MutableRefObject, ReactElement, useEffect, useRef } from 'react';
import * as ReactDOM from 'react-dom/client';
import cytoscape, { Css, NodeSingular } from 'cytoscape';
import dagre, { DagreLayoutOptions } from 'cytoscape-dagre';
import popper from 'cytoscape-popper';
import tippy, { Instance } from 'tippy.js';

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

// Stable fallbacks for optional props. Object/function literals used directly as default
// parameter values are re-created on every call when the caller omits the prop. For a caller
// that skips the memo below entirely (or that only compares some props, the way it does here —
// see the comment on the memo export), an unstable default can defeat referential-equality checks
// and cause unnecessary Cytoscape rebuilds, so these are hoisted defensively even though the
// current caller (RulesVisualizerView) happens to always pass its own stable values.
const EMPTY_NODE_STYLE: Css.Node = {};
const EMPTY_EDGE_STYLE: Css.Edge = {};
const EMPTY_LAYOUT_OPTIONS: Partial<DagreLayoutOptions> = {};
const NOOP_ON_LOAD = () => {};

const HierarchicalGraph = ({
  elements,
  nodeStyle = EMPTY_NODE_STYLE,
  edgeStyle = EMPTY_EDGE_STYLE,
  layoutOptions = EMPTY_LAYOUT_OPTIONS,
  onLoad = NOOP_ON_LOAD,
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

    let tip: Instance | undefined;
    if (ToolTip) {
      cy.nodes().bind('mouseover', (event) => {
        tip = renderToolTipWithTippy(
          event.target as NodeSingular,
          ToolTip,
          containerRef,
          toolTipRef,
          tooltipRootRef
        ) as Instance | undefined;
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
    const tip: unknown = tippy(containerRef.current, {
      getReferenceClientRect: popperRef.getBoundingClientRect,
      content: toolTipRef.current,
      placement: 'bottom',
      arrow: true,
    });
    (tip as { show: () => void }).show();
    return tip;
  }
}

// NOTE: `elements` is deep-compared (JSON.stringify), so a fresh object reference with the same
// content is fine here. `nodeStyle`/`edgeStyle`/`layoutOptions`/`ToolTip` are compared by
// reference (`==`) — passing any of these as a fresh object/array literal from the parent on
// every render defeats this memo and forces a full Cytoscape rebuild (destroying pan/zoom state)
// on every parent re-render, not just when the graph actually changes. `onLoad` isn't compared at
// all, so its stability doesn't affect this memo either way (though it's still in the effect's own
// dependency list below, so an unstable `onLoad` can matter if the component re-renders for some
// other reason).
export default memo(HierarchicalGraph, (prevProps, nextProps) => {
  return (
    JSON.stringify(prevProps.elements) == JSON.stringify(nextProps.elements) &&
    prevProps.nodeStyle == nextProps.nodeStyle &&
    prevProps.edgeStyle == nextProps.edgeStyle &&
    prevProps.layoutOptions == nextProps.layoutOptions &&
    prevProps.ToolTip == nextProps.ToolTip
  );
});
