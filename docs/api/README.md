# UI API Reference

The Osprey UI API is the HTTP service (Flask, port `5004`) behind the Osprey web UI. Its OpenAPI 3.1 description is [`openapi.yaml`](openapi.yaml). The gRPC interfaces are described by the `.proto` files under `proto/osprey/rpc/` instead.

Things to know before you call it:

- **Every route is served twice**, at the root and under `/api` (`GET /config` and `GET /api/config`). `/_health` and `/_debug_sentry` are root-only.
- **There is no authentication.** The caller's identity is whatever the `X-Test-Email` header says, falling back to the `OSPREY_DEV_USER_EMAIL` environment variable and then `local-dev@localhost`. Access to each endpoint is gated by *abilities* configured for that email, and a missing ability returns `401`. Do not expose the API without an authenticating proxy in front of it.
- **Error bodies are not uniform.** Depending on where a request fails, the body is a plain string, a JSON array of validation errors, or a rendered SML validation failure; all are sent with a JSON content type.
- Some bulk-action and bulk-label endpoints are stubs that always return `501`.

View the spec with any OpenAPI tool, for example:

```sh
npx @redocly/cli@2.71.0 preview-docs docs/api/openapi.yaml
```

## Changing the API

Keep `openapi.yaml` in step with the routes under `osprey_worker/src/osprey/worker/ui_api/osprey/views/`. CI lints it with Redocly CLI; run the same check locally:

```sh
npx @redocly/cli@2.71.0 lint --config docs/api/redocly.yaml docs/api/openapi.yaml
```
