import * as React from 'react';
import { ResizeObserver, ResizeObserverEntry } from '@juggle/resize-observer';

export default function useResizeObserver(
  element: HTMLElement | null,
  onUpdate?: (entry: ResizeObserverEntry) => void
): DOMRect | null {
  const [refRect, setRefRect] = React.useState<DOMRect | null>(null);

  React.useLayoutEffect(() => {
    if (element == null) return;

    const resizeObserver = new ResizeObserver((entries) => {
      const entry = entries[0];
      setRefRect(entry.target.getBoundingClientRect());
      onUpdate?.(entry);
    });

    resizeObserver.observe(element);

    return () => resizeObserver.disconnect();
  }, [onUpdate, element]);

  return refRect;
}
