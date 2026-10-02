import { describe, it, expect } from '@rstest/core';

import { MIN_ZOOM_FIT_MULTIPLIER, computeMinZoom, computeZoomPercentage } from './zoomUtils';

describe('computeMinZoom', () => {
  it('scales the fit zoom down by the configured multiplier', () => {
    expect(computeMinZoom(2)).toBeCloseTo(2 * MIN_ZOOM_FIT_MULTIPLIER);
  });

  it('handles a fit zoom below 1 (large graphs zoomed way out to fit)', () => {
    expect(computeMinZoom(0.05)).toBeCloseTo(0.05 * MIN_ZOOM_FIT_MULTIPLIER);
  });

  it('handles a fit zoom above 1 (small graphs zoomed in to fill the viewport)', () => {
    expect(computeMinZoom(8.89)).toBeCloseTo(8.89 * MIN_ZOOM_FIT_MULTIPLIER);
  });
});

describe('computeZoomPercentage', () => {
  it('reads 100% when the current zoom equals the initial fit zoom', () => {
    expect(computeZoomPercentage(8.89, 8.89)).toBe(100);
  });

  it('reads 100% regardless of how large the raw fit zoom units are, as long as zoom matches base', () => {
    expect(computeZoomPercentage(0.05, 0.05)).toBe(100);
  });

  it('reads below 100% when zoomed out past the fit level', () => {
    expect(computeZoomPercentage(1, 2)).toBe(50);
  });

  it('reads above 100% when zoomed in past the fit level', () => {
    expect(computeZoomPercentage(3, 2)).toBe(150);
  });

  it('rounds to the nearest whole percent', () => {
    expect(computeZoomPercentage(1, 3)).toBe(33);
  });
});
