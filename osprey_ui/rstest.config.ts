import { pluginReact } from '@rsbuild/plugin-react';
import { defineConfig } from '@rstest/core';

// Ask AI panel tests render React components, which need a DOM and the React JSX
// transform. Rstest only supports a global test environment (no per-file override), so
// the whole UI test suite runs under happy-dom; pure-logic tests are unaffected. The
// setup file polyfills a couple of browser APIs Ant Design expects.
export default defineConfig({
  plugins: [pluginReact()],
  testEnvironment: 'happy-dom',
  setupFiles: ['./rstest.setup.ts'],
});
