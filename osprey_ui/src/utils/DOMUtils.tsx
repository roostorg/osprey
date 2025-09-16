import { CSSProperties } from 'react';

const MARGIN = 12;
const ARROW_ICON_SPACING = 6;
const ARROW_ICON_SIZE = 12;
// The arrow icon is actually a diamond, only 2px of it should be peeping out from behind a tooltip.
const ARROW_ICON_SHOWN = 2;

export enum Placements {
  RIGHT_TOP = 'RIGHT_TOP',
  RIGHT_BOTTOM = 'RIGHT_BOTTOM',
  LEFT_TOP = 'LEFT_TOP',
  LEFT_BOTTOM = 'LEFT_BOTTOM',
  TOP_LEFT = 'TOP_LEFT',
  TOP_RIGHT = 'TOP_RIGHT',
  BOTTOM_LEFT = 'BOTTOM_LEFT',
  BOTTOM_RIGHT = 'BOTTOM_RIGHT',
}

enum Position {
  TOP = 'TOP',
  BOTTOM = 'BOTTOM',
  LEFT = 'LEFT',
  RIGHT = 'RIGHT',
}

enum Align {
  TOP = 'TOP',
  BOTTOM = 'BOTTOM',
  LEFT = 'LEFT',
  RIGHT = 'RIGHT',
}

const PlacementProps: Record<Position, 'top' | 'left'> = {
  [Position.TOP]: 'top',
  [Position.BOTTOM]: 'top',
  [Position.LEFT]: 'left',
  [Position.RIGHT]: 'left',
};

const PlacementDimensions: Record<Position, 'height' | 'width'> = {
  [Position.TOP]: 'height',
  [Position.BOTTOM]: 'height',
  [Position.LEFT]: 'width',
  [Position.RIGHT]: 'width',
};

function _getCorrected<T>(current: T, incorrect: T, alternate: T): T {
  return current === incorrect ? alternate : current;
}

function _getValuesFromPlacement(placement: Placements): { position: Position; align: Align } {
  const [positionStr, alignStr] = placement.split('_');
  const position = Position[positionStr as Position];
  const align = Align[alignStr as Align];

  return { position, align };
}

function _fitPopoutToWindow(
  popoutClientRect: DOMRect,
  popoutStyle: CSSProperties,
  { position, align }: { position: Position; align: Align }
): Placements {
  let correctedPosition = position;
  let correctedAlign = align;

  const left = popoutStyle.left != null ? Number(popoutStyle.left) : 0;
  const top = popoutStyle.top != null ? Number(popoutStyle.top) : 0;

  const isTooFarRight = left + popoutClientRect.width > window.innerWidth;
  const isTooFarTop = top < 0;
  const isTooFarLeft = left < 0;
  const isTooFarBottom = top + popoutClientRect.height > window.innerHeight;

  if (isTooFarTop) {
    correctedPosition = _getCorrected(position, Position.TOP, Position.BOTTOM);
    correctedAlign = _getCorrected(align, Align.BOTTOM, Align.TOP);
  } else if (isTooFarBottom) {
    correctedPosition = _getCorrected(position, Position.BOTTOM, Position.TOP);
    correctedAlign = _getCorrected(align, Align.TOP, Align.BOTTOM);
  }

  if (isTooFarLeft) {
    correctedPosition = _getCorrected(correctedPosition, Position.LEFT, Position.RIGHT);
    correctedAlign = _getCorrected(correctedAlign, Align.RIGHT, Align.LEFT);
  } else if (isTooFarRight) {
    correctedPosition = _getCorrected(correctedPosition, Position.RIGHT, Position.LEFT);
    correctedAlign = _getCorrected(correctedAlign, Align.LEFT, Align.RIGHT);
  }

  const newPlacement = `${correctedPosition}_${correctedAlign}`;
  return newPlacement as Placements;
}

export function getPopoutStyle(
  clientRect: DOMRect,
  popoutClientRect: DOMRect | null,
  placement: Placements = Placements.RIGHT_BOTTOM,
  isCorrected: boolean = false
): { popoutStyle: React.CSSProperties; arrowStyle: React.CSSProperties } {
  const popoutStyle: React.CSSProperties = {};
  const arrowStyle: React.CSSProperties = {};
  if (popoutClientRect == null) return { popoutStyle, arrowStyle };
  const { position, align } = _getValuesFromPlacement(placement);

  const posProp = PlacementProps[position];
  const alignProp = PlacementProps[align];

  const posDim = PlacementDimensions[position];
  const alignDim = PlacementDimensions[align];

  if (position === Position.TOP || position === Position.LEFT) {
    popoutStyle[posProp] = clientRect[posProp] - popoutClientRect[posDim] - MARGIN;
    arrowStyle[posProp] = popoutClientRect[posDim] - ARROW_ICON_SIZE + ARROW_ICON_SHOWN;
  } else {
    popoutStyle[posProp] = clientRect[posProp] + clientRect[posDim] + MARGIN;
    arrowStyle[posProp] = -ARROW_ICON_SHOWN;
  }

  if (align === Align.TOP || align === Align.LEFT) {
    popoutStyle[alignProp] = clientRect[alignProp];
    arrowStyle[alignProp] = ARROW_ICON_SPACING;
  } else {
    popoutStyle[alignProp] = clientRect[alignProp] + clientRect[alignDim] - popoutClientRect[alignDim];
    arrowStyle[alignProp] = popoutClientRect[alignDim] - ARROW_ICON_SPACING - ARROW_ICON_SIZE;
  }

  if (!isCorrected) {
    const correctedPlacement = _fitPopoutToWindow(popoutClientRect, popoutStyle, { position, align });
    return getPopoutStyle(clientRect, popoutClientRect, correctedPlacement, true);
  }

  popoutStyle.width = popoutClientRect.width;
  return { popoutStyle, arrowStyle };
}
