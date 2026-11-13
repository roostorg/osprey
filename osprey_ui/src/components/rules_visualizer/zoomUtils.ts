// Must match the recenter button's fit padding, or recentering won't return to exactly 100%.
export const GRAPH_FIT_PADDING = 30;

// Fraction of the initial fit zoom that users can zoom out to.
export const MIN_ZOOM_FIT_MULTIPLIER = 0.3;

// Relative amount each +/- zoom control click changes the zoom level by.
export const ZOOM_STEP_FACTOR = 1.2;

export function computeMinZoom(fitZoom: number): number {
  return fitZoom * MIN_ZOOM_FIT_MULTIPLIER;
}

// Percentage relative to the initial fit zoom, so 100% always means "the whole graph fits".
export function computeZoomPercentage(zoom: number, baseZoom: number): number {
  return Math.round((zoom / baseZoom) * 100);
}
