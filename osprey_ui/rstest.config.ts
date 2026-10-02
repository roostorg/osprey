import { defineConfig } from '@rstest/core';
import { pluginReact } from '@rsbuild/plugin-react';

export default defineConfig({
  testEnvironment: 'jsdom',
  plugins: [pluginReact()],
});
