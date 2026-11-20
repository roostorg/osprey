import { defineConfig, loadEnv } from '@rsbuild/core';
import { pluginReact } from '@rsbuild/plugin-react';
import { pluginSass } from '@rsbuild/plugin-sass';

const { publicVars, rawPublicVars } = loadEnv({ prefixes: ['REACT_APP_'] });

// The UI API also serves every route under /api. Proxying that prefix lets the UI call the
// API on its own origin (REACT_APP_API_BASE_URL=/api), which is what works behind
// forwarded-port hosts such as GitHub Codespaces, where a second port is a separate,
// login-protected origin.
const apiProxyTarget = process.env.OSPREY_UI_API_PROXY_TARGET ?? 'http://localhost:5004';

export default defineConfig({
  plugins: [pluginReact(), pluginSass()],
  server: {
    host: '0.0.0.0',
    port: 5002,
    proxy: {
      '/api': apiProxyTarget,
    },
  },
  html: {
    template: './public/index.html',
  },
  output: {
    distPath: {
      root: 'build',
    },
  },
  source: {
    define: {
      ...publicVars,
      'process.env': JSON.stringify(rawPublicVars),
    },
  },
});
