import { describe, it, expect, rstest, beforeEach } from '@rstest/core';
import { render, screen, fireEvent, cleanup } from '@testing-library/react';
import type { Core } from 'cytoscape';

import useRulesVisualizerStore from '../../stores/RulesVisualizerStore';
import { NodeType } from '../../types/RulesVisualizerTypes';
import RulesVisualizerView from './RulesVisualizer';
import { computeMinZoom } from './zoomUtils';

// Cytoscape draws to a real <canvas>, unsupported by jsdom, so this fakes just enough of its API.
// MAX_SANE_CONSTRUCTIONS guards against a render loop hanging or OOMing the test worker instead
// of failing fast (onLoad calls setState synchronously on every construction).
let constructCount = 0;
const MAX_SANE_CONSTRUCTIONS = 5;
// Exposes the most recently created mock so tests can assert on calls like `minZoom`.
let lastMockInstance: ReturnType<typeof buildMockCyInstance>;

function buildMockCyInstance() {
  let zoom = 2; // arbitrary "fit" zoom cytoscape would have picked for this graph
  let minZoomValue = 0;
  const zoomHandlers: Array<() => void> = [];

  const instance = {
    // Real Cytoscape clamps the zoom setter to [minZoom, maxZoom]; mirrored here since the min
    // clamp is the actual behavior under test (the PR's zoom-out floor).
    zoom: rstest.fn((arg?: number | { level: number }) => {
      if (arg === undefined) {
        return zoom;
      }
      const requested = typeof arg === 'number' ? arg : arg.level;
      zoom = Math.max(requested, minZoomValue);
      zoomHandlers.forEach((handler) => handler());
      return instance;
    }),
    minZoom: rstest.fn((value?: number) => {
      if (value !== undefined) {
        minZoomValue = value;
      }
      return minZoomValue;
    }),
    maxZoom: rstest.fn(),
    on: rstest.fn((event: string, handler: () => void) => {
      if (event === 'zoom') {
        zoomHandlers.push(handler);
      }
    }),
    animate: rstest.fn((opts: { fit?: { padding?: number } }) => {
      // Recenter: re-fit to the load zoom, but only with the expected padding.
      if (opts.fit?.padding === 30) {
        zoom = 2;
        zoomHandlers.forEach((handler) => handler());
      }
    }),
    elements: rstest.fn(() => ({})),
    nodes: rstest.fn(() => ({ bind: rstest.fn(), unbind: rstest.fn() })),
    width: rstest.fn(() => 800),
    height: rstest.fn(() => 600),
    destroy: rstest.fn(),
  };

  return instance;
}

function createMockCyInstance(): Core {
  constructCount += 1;
  if (constructCount > MAX_SANE_CONSTRUCTIONS) {
    throw new Error(
      `cytoscape() was constructed ${constructCount} times in one test — this looks like an ` +
        'unstable-prop render loop (check layoutOptions/nodeStyle/edgeStyle/ToolTip reference ' +
        "stability against HierarchicalGraph's memo comparator), not a real graph reload."
    );
  }
  lastMockInstance = buildMockCyInstance();
  return lastMockInstance as unknown as Core;
}

rstest.mock('cytoscape', () => {
  const cytoscapeMock = rstest.fn(() => createMockCyInstance());
  (cytoscapeMock as unknown as { use: () => void }).use = rstest.fn();
  return { __esModule: true, default: cytoscapeMock };
});

const seedGraph = () => {
  useRulesVisualizerStore.setState({
    nodes: [
      {
        id: 1,
        name: 'ContainsHello',
        num_children: 0,
        type: NodeType.Rule,
        file_path: 'example_rules/rules/post_contains_hello.sml',
        value: 'ContainsHello',
      },
    ],
    edges: [],
    selectedFeature: 'ContainsHello',
    selectedFeatureType: 'Action',
  });
};

// 200 nodes / ~200 edges, exercising the same wiring at a scale closer to a real dense ruleset.
const seedLargeGraph = () => {
  const ruleCount = 100;
  const labelCount = 100;
  const nodes = [
    ...Array.from({ length: ruleCount }, (_, i) => ({
      id: i,
      name: `Rule${i}`,
      num_children: 1,
      type: NodeType.Rule,
      file_path: 'not-a-real-file.sml',
      value: `Rule${i}`,
    })),
    ...Array.from({ length: labelCount }, (_, i) => ({
      id: ruleCount + i,
      name: `label_${i}`,
      num_children: 0,
      type: NodeType.Label,
      label_type: 'LabelAdd',
      label_name: `label_${i}`,
      entity_name: 'UserId',
      file_path: 'not-a-real-file.sml',
      value: `label_${i}`,
    })),
  ];
  // Each rule checks the previous rule's label and adds its own, forming a long fan-through chain.
  const edges = Array.from({ length: ruleCount }, (_, i) => [
    { source: ruleCount + i, target: i, weight: 1, color: '#000' },
    { source: i, target: ruleCount + ((i + 1) % labelCount), weight: 1, color: '#000' },
  ]).flat();

  useRulesVisualizerStore.setState({
    nodes,
    edges,
    selectedFeature: 'label_0',
    selectedFeatureType: 'Label',
  });
};

beforeEach(() => {
  constructCount = 0;
  useRulesVisualizerStore.setState({
    nodes: null,
    edges: null,
    selectedFeature: undefined,
    selectedFeatureType: undefined,
  });
  cleanup();
});

describe('RulesVisualizerView zoom controls', () => {
  it('shows 100% on load and builds the graph exactly once', () => {
    seedGraph();
    render(<RulesVisualizerView />);

    screen.getByText('100%');
    expect(constructCount).toBe(1);
  });

  it('does not rebuild the Cytoscape instance as the zoom-percentage display updates', () => {
    // Regression test: an unstable `layoutOptions` reference used to defeat HierarchicalGraph's
    // memo and rebuild the graph on every zoom-triggered render (the "rubberbanding" bug).
    seedGraph();
    render(<RulesVisualizerView />);
    expect(constructCount).toBe(1);

    fireEvent.click(screen.getByRole('button', { name: 'Zoom in' }));
    fireEvent.click(screen.getByRole('button', { name: 'Zoom in' }));
    fireEvent.click(screen.getByRole('button', { name: 'Zoom out' }));

    expect(constructCount).toBe(1);
  });

  it('updates the displayed percentage as zoom changes, and recentering returns to exactly 100%', () => {
    seedGraph();
    render(<RulesVisualizerView />);

    fireEvent.click(screen.getByRole('button', { name: 'Zoom in' }));
    screen.getByText('120%');

    fireEvent.click(screen.getByRole('button', { name: 'Recenter' }));
    screen.getByText('100%');
  });

  it('sets minZoom to 30% of the fit zoom, and clamps zooming out at that floor', () => {
    seedGraph();
    render(<RulesVisualizerView />);

    expect(lastMockInstance.minZoom).toHaveBeenCalledWith(computeMinZoom(2));

    // Fit zoom is 2, so the floor is 0.6 (30%). Enough clicks to overshoot it in raw zoom
    // terms — the clamp should hold at exactly 30%, not go lower.
    for (let i = 0; i < 10; i++) {
      fireEvent.click(screen.getByRole('button', { name: 'Zoom out' }));
    }

    screen.getByText('30%');
  });

  it('handles a large, dense graph the same way as a small one', () => {
    seedLargeGraph();
    render(<RulesVisualizerView />);

    screen.getByText('100%');
    expect(constructCount).toBe(1);

    fireEvent.click(screen.getByRole('button', { name: 'Zoom out' }));
    fireEvent.click(screen.getByRole('button', { name: 'Zoom out' }));
    screen.getByText('69%');
    expect(constructCount).toBe(1);

    fireEvent.click(screen.getByRole('button', { name: 'Recenter' }));
    screen.getByText('100%');
  });
});
