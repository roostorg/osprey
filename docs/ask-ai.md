# Ask AI

Osprey ships a **reusable, vendor-neutral Ask AI stack**: an LLM provider surface, a
host-neutral turn-orchestration service, a versioned Server-Sent Events (SSE) transport,
and a generic React chat panel. Everything product- or vendor-specific (identity, tools,
grounding, redaction, persistence, endpoint wiring, model config, UI placement) is
supplied by the embedding **host** through explicit adapters — the generic packages
import no product identity, product stores/query types, or concrete vendor SDK.

> Consuming this capability from a specific product (e.g. wiring it to that product's
> auth, stores, and query tools) is a separate, downstream integration and is not part of
> the generic packages described here.

## 1. LLM provider surface (`osprey.worker.lib.llm`)

Vendor-neutral message/tool/response dataclasses plus `BaseLLMProvider`, a `ToolRegistry`,
and a bounded `run_tool_loop`. A provider is registered through the Pluggy hook
`register_llm_provider(config) -> BaseLLMProvider` (`firstresult=True`) and obtained via
`osprey.worker.adaptor.plugin_manager.bootstrap_llm_provider(config)`, which returns
`None` when no provider plugin is installed. **Core has no vendor SDK dependency.**

A runnable example lives in `example_plugins` (`llm.anthropic_provider.AnthropicLLMProvider`,
registered via a `register_llm_provider` hookimpl); its `anthropic` SDK dependency is
confined to `example_plugins` and imported lazily, so discovery never requires the SDK.

## 2. Ask service (`osprey.worker.lib.ask`)

`AskService(AskConfig).run_turn(request, principal, *, cancel=None)` is a generator that
pre-flights (raising typed errors before emitting anything), then streams a deterministic,
bounded sequence of events over the provider and a principal-scoped tool registry,
persists a successful turn, and yields exactly one terminal event. The conversation lock
is released on every terminal path (including client disconnect).

`AskConfig` bundles the host ports (all Protocols in `ask.ports`):

| Port | Responsibility |
|---|---|
| `ModelPolicy` | Validate the model; select **and ready** a provider; return per-turn limits. Absence/init-failure → `unavailable_provider`. |
| `ConversationStore` | Load/create conversations (enforcing principal ownership); persist a successful turn only. |
| `ConversationLock` | Serialize turns per conversation; released on every terminal path. |
| `ContextSnapshotProvider` (opt) | Validate + render a `context_ref` to grounding text. Grounding only — never authorization. |
| `ToolRegistryFactory` | Build a principal-scoped `ToolRegistry` for the turn. |
| `Redactor` (opt) | `redact_text` + `redact_payload` applied before events cross the boundary. |
| `AuditSink` (opt) | Record terminal outcomes (best-effort). |
| `EvidenceNormalizer` (opt) | Map a (tool_call, tool_result) to a `query_result` payload. |

**Invariants a host must uphold:** the `Principal` is server-derived (never from the
request body); context cannot grant authorization; tools enforce permissions
independently; persistence and locking are mandatory (no process-global state); redaction
happens before the transport boundary.

## 3. SSE transport (`osprey.worker.ui_api.osprey.lib.ask`)

`create_ask_blueprint(name, service_factory, principal_provider, url='/ask')` returns a
Flask blueprint with one POST route that streams `AskEvent`s as SSE. The host supplies
`service_factory` (builds an `AskService` from its adapters) and `principal_provider`
(derives the server principal), and chooses where to register the blueprint. Pre-flight
failures return an HTTP 4xx/5xx JSON error; once streaming starts, exactly one terminal
`done`/`error` event is emitted, and unexpected host-adapter failures become a safe JSON
500 rather than a raw stack trace.

The versioned event schema (`version`, `type`, `conversation_id`, `turn_id`, `payload`)
covers `conversation_started`, `tool_call`, `query_result`, `assistant_message`, `done`,
and `error`. Assistant text is delivered as one complete `assistant_message` per turn
(the provider surface is non-streaming; token-level streaming is a future extension).

## 4. Frontend (`osprey_ui/src/components/ask_ai`)

A generic, host-mountable panel:

```tsx
import { AskAiPanel } from 'components/ask_ai';

<AskAiPanel
  endpoint={{ url: '/api/ask' }}
  models={[{ value: 'model-a', label: 'Model A' }]}
  getContextSnapshot={() => currentContextRef}
  renderEvidence={(evidence) => <MyEvidenceCard evidence={evidence} />}
/>;
```

It owns typed wire/domain contracts, a chunk-safe POST-SSE client (`streamAsk`), a Zustand
store (`useAskStore` / `createAskStore`) with a stale-request guard and abort handling, and
safe markdown rendering. Without a `renderEvidence` adapter it shows query steps as a
read-only card. It imports no product stores, product query types, product identity, or
product UI kit, and is not wired into any product's navigation.
