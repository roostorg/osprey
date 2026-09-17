import { describe, it, expect, rstest, beforeEach } from '@rstest/core';
import { render, screen, fireEvent, cleanup } from '@testing-library/react';
import type { Core } from 'cytoscape';

import useRulesVisualizerStore from '../../stores/RulesVisualizerStore';
import { NodeType } from '../../types/RulesVisualizerTypes';
import RulesVisualizerView from './RulesVisualizer';

// Cytoscape draws to a real <canvas>, which jsdom doesn't implement, so we replace it with a
// minimal fake that tracks the same things RulesVisualizer.tsx actually calls: zoom get/set (and
// firing the 'zoom' event listener it registers), minZoom, animate/fit, and instance construction
// count. That last one is what actually matters here — see the "stays mounted" test below.
//
// A regression here isn't just "wrong count": HierarchicalGraph's onLoad calls setState
// synchronously on every construction, which — if something makes the graph rebuild on every
// render — recurses forever (confirmed by hand: this genuinely OOM-crashes the test worker rather
// than failing a quick assertion). MAX_SANE_CONSTRUCTIONS turns that into a clear, fast failure.
let constructCount = 0;
const MAX_SANE_CONSTRUCTIONS = 5;

function createMockCyInstance() {
  constructCount += 1;
  if (constructCount > MAX_SANE_CONSTRUCTIONS) {
    // Fail fast and loud instead of spinning: see the comment on MAX_SANE_CONSTRUCTIONS above.
    throw new Error(
      `cytoscape() was constructed ${constructCount} times in one test — this looks like an ` +
        'unstable-prop render loop (check layoutOptions/nodeStyle/edgeStyle/ToolTip reference ' +
        "stability against HierarchicalGraph's memo comparator), not a real graph reload."
    );
  }
  let zoom = 2; // arbitrary "fit" zoom cytoscape would have picked for this graph
  const zoomHandlers: Array<() => void> = [];

  const instance = {
    zoom: rstest.fn((arg?: number | { level: number }) => {
      if (arg === undefined) {
        return zoom;
      }
      zoom = typeof arg === 'number' ? arg : arg.level;
      zoomHandlers.forEach((handler) => handler());
      return instance;
    }),
    minZoom: rstest.fn(),
    maxZoom: rstest.fn(),
    on: rstest.fn((event: string, handler: () => void) => {
      if (event === 'zoom') {
        zoomHandlers.push(handler);
      }
    }),
    animate: rstest.fn((opts: { fit?: { padding?: number } }) => {
      // Recentering: mimic Cytoscape re-fitting to the same zoom the graph loaded at, as long as
      // the same padding is used (this is the actual invariant the recenter bug broke).
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

  return instance as unknown as Core;
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

    expect(screen.getByText('100%')).toBeTruthy();
    expect(constructCount).toBe(1);
  });

  it('does not rebuild the Cytoscape instance as the zoom-percentage display updates', () => {
    // Regression test for the "rubberbanding" bug: HierarchicalGraph is wrapped in a memo that
    // compares `layoutOptions` (among other props) by reference equality. Passing a fresh object
    // literal for `layoutOptions` on every render of RulesVisualizerView defeated that memo, so
    // every zoom-triggered re-render destroyed and recreated the whole graph, snapping the view
    // back to 100%. Confirmed by hand that reverting `layoutOptions` to an inline literal makes
    // this test fail (loudly — see MAX_SANE_CONSTRUCTIONS above); `elements` is deep-compared by
    // the same memo, so it doesn't need referential stability for this specific bug, and `onLoad`
    // isn't compared by the memo at all. Both are still memoized/useCallback'd in the component
    // for hygiene, since they're re-read by the effect's own dependency array if it ever re-runs
    // for an unrelated reason.
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
    expect(screen.getByText('120%')).toBeTruthy();

    fireEvent.click(screen.getByRole('button', { name: 'Recenter' }));
    expect(screen.getByText('100%')).toBeTruthy();
  });
});
