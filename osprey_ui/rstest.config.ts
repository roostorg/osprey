import { defineConfig } from '@rstest/core';
import { pluginReact } from '@rsbuild/plugin-react';

// Compile .tsx with the automatic JSX runtime, matching rsbuild.config.ts.
export default defineConfig({
  plugins: [pluginReact()],
});
