// Padding (in px) used for both the initial dagre layout fit and the recenter button's fit.
// These must match — otherwise the two fits compute different zoom levels for the same
// elements/viewport, and recentering doesn't return to the original 100% view.
export const GRAPH_FIT_PADDING = 30;

// How far below the initial fit zoom users are allowed to zoom out, as a fraction of that fit
// zoom (e.g. 0.3 lets users zoom out to 30% the size of the initial whole-graph fit).
export const MIN_ZOOM_FIT_MULTIPLIER = 0.3;

// Relative amount each +/- zoom control click changes the zoom level by.
export const ZOOM_STEP_FACTOR = 1.2;

export function computeMinZoom(fitZoom: number): number {
  return fitZoom * MIN_ZOOM_FIT_MULTIPLIER;
}

// Displayed zoom percentage is relative to the initial fit zoom, so it always reads 100% when
// the whole graph fits, regardless of what that fit zoom level actually is in Cytoscape's own
// units (a small graph fit to screen can have a raw Cytoscape zoom far above or below 1).
export function computeZoomPercentage(zoom: number, baseZoom: number): number {
  return Math.round((zoom / baseZoom) * 100);
}
