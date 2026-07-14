// Polyfills for Ant Design under happy-dom. Guards are no-ops when happy-dom already
// provides the API.

if (typeof globalThis.matchMedia !== 'function') {
  globalThis.matchMedia = ((query: string) => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: () => undefined,
    removeListener: () => undefined,
    addEventListener: () => undefined,
    removeEventListener: () => undefined,
    dispatchEvent: () => false,
  })) as unknown as typeof globalThis.matchMedia;
}

if (typeof globalThis.ResizeObserver === 'undefined') {
  globalThis.ResizeObserver = class {
    observe(): void {
      /* no-op */
    }

    unobserve(): void {
      /* no-op */
    }

    disconnect(): void {
      /* no-op */
    }
  } as unknown as typeof globalThis.ResizeObserver;
}
