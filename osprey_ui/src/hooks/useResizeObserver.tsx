import * as React from 'react';
import { ResizeObserver, ResizeObserverEntry } from '@juggle/resize-observer';

type RefOrElement = React.RefObject<HTMLElement> | HTMLElement | null;

export default function useResizeObserver(
  element: RefOrElement,
  onUpdate?: (entry: ResizeObserverEntry) => void
): DOMRect | null {
  const [refRect, setRefRect] = React.useState<DOMRect | null>(null);

  React.useLayoutEffect(() => {
    // Handle both ref objects and element values
    const el = element && 'current' in element ? element.current : element;
    if (el == null) return;

    const resizeObserver = new ResizeObserver((entries) => {
      const entry = entries[0];
      setRefRect(entry.target.getBoundingClientRect());
      onUpdate?.(entry);
    });

    resizeObserver.observe(el);

    return () => resizeObserver.disconnect();
  }, [onUpdate, element]);

  return refRect;
}
