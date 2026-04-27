import { defineConfig, loadEnv } from '@rsbuild/core';
import { pluginReact } from '@rsbuild/plugin-react';
import { pluginSass } from '@rsbuild/plugin-sass';
import { pluginNodePolyfill } from '@rsbuild/plugin-node-polyfill';

const { publicVars } = loadEnv({ prefixes: ['REACT_APP_'] });

export default defineConfig({
  plugins: [pluginReact(), pluginSass(), pluginNodePolyfill()],
  server: {
    host: '0.0.0.0',
    port: 5002,
  },
  html: {
    template: './public/index.html',
  },
  output: {
    polyfill: 'usage',
    distPath: {
      root: 'build',
    },
  },
  source: {
    define: publicVars,
    include: [{ not: /[\\/]core-js[\\/]/ }],
  },
});
