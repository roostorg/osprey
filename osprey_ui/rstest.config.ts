import { defineConfig } from '@rstest/core';

export default defineConfig({
  coverage: {
    // Off for `pnpm run test`; `pnpm run test:coverage` turns it on.
    enabled: false,
    // Measure all of src, not just the files the tests import, so the number reflects the whole UI.
    include: ['src/**/*.{js,jsx,ts,tsx}'],
    // Paths relative to the repo root (`osprey_ui/src/...`) so they match repo paths in Codecov.
    reporters: ['text-summary', ['lcov', { projectRoot: '..' }]],
  },
});
