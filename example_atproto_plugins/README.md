# example_atproto_plugins

A sample Osprey plugin that consumes ATProto's [JetStream](https://docs.bsky.app/blog/jetstream) as the input event source. It gives you:

- a `register_input_stream` hook implementation that subscribes to JetStream over WebSocket and yields Osprey `Action`s with the JetStream JSON event passed through as-is,
- realistic per-second event volume from the live Bluesky network, which is useful for load and soak testing changes that the synthetic 1-event/second producer doesn't exercise,
- a companion `example_atproto_rules/` tree showing how to organize rules against ATProto event shapes, with file structure modeled on [haileyok/atproto-ruleset](https://github.com/haileyok/atproto-ruleset).

This package registers **only the input stream**. The sample rules also use a UDF (`TextContains`), a labels service, and an output sink that are provided by the sibling `example_plugins/` package, so the two run together: the worker image installs both, and Osprey loads every registered plugin, so `example_plugins` supplies those pieces automatically in the docker stack. If you lift this sample into a setup without `example_plugins`, provide those yourself (a labels provider and output sink) or restrict the rules to stdlib UDFs.

## Running

From the repo root:

```sh
./run-atproto.sh
```

This brings up the full Osprey local stack (Druid, Postgres, MinIO, Kafka) along with a JetStream websocket override, and swaps the worker's input source from Kafka to the JetStream plugin, pointing it at `example_atproto_rules` instead of `example_rules`. First-run startup takes a few minutes.

## Configuration

| Env var | Default | Description |
| --- | --- | --- |
| `OSPREY_INPUT_STREAM_SOURCE` | (must be) `plugin` | Selects the plugin-provided stream. |
| `OSPREY_JETSTREAM_ENDPOINT` | `wss://jetstream2.us-west.bsky.network/subscribe` | JetStream WebSocket URL. |
| `OSPREY_JETSTREAM_WANTED_COLLECTIONS` | `app.bsky.feed.post,app.bsky.feed.like,app.bsky.feed.repost,app.bsky.graph.follow,app.bsky.actor.profile` | Comma-separated collections to subscribe to (server-side filter). |

## Action shape

The JetStream JSON event is passed through unchanged as the Action's `data` dict, so rules read JetStream-native paths directly. `action_name` is `<operation>_<short>` for commit events (`create_post`, `delete_like`, `update_profile`, …) using the short names defined in `COLLECTION_NAMES`, or `identity` for identity events.

### Commit events (e.g. `create_post`, `delete_like`)

```
{
  "did": "did:plc:...",
  "time_us": 1714500000000000,
  "kind": "commit",
  "commit": {
    "rev": "...",
    "operation": "create" | "update" | "delete",
    "collection": "app.bsky.feed.post",
    "rkey": "...",
    "cid": "...",
    "record": { ... raw ATProto record ... }
  }
}
```

### Identity events (`action_name='identity'`)

```
{
  "did": "did:plc:...",
  "time_us": ...,
  "kind": "identity",
  "identity": {"did": "...", "handle": "...", "seq": ..., "time": "..."}
}
```

Account events, commits for collections not in `COLLECTION_NAMES`, and commits with operations other than `create` / `update` / `delete` are skipped.

### UI default features

`example_atproto_rules/config/ui_config.yaml` declares the per-action default features the Osprey UI surfaces in the event stream — e.g. `PostText` for `create_post`, `IdentityHandle` for `identity`, `Subject` for like / repost / follow events. Add new entries there to expose more fields without touching rule code.

`action_id` is minted from `snowflake-id-worker` in batches of 250. The plugin therefore needs `SNOWFLAKE_API_ENDPOINT` to be set (the local docker-compose stack provides it).

## Caveats

- **Not production-ready.** No durable cursor on process restart, no zstd compression, no DID-level filtering. Good for sample / load-testing purposes; not a drop-in for a real ATProto deployment.
- **No event enrichment.** JetStream only carries what's in the commit itself; rulesets that depend on handle / profile / account age (such as much of [atproto-ruleset](https://github.com/haileyok/atproto-ruleset)) are fed by a separate enrichment pipeline, not JetStream directly. This plugin emits JetStream-native paths ($.did, $.commit.collection, etc.); enrichment-fed rulesets would need an enrichment service in front of this one or a different plugin.
- **Connection health.** WebSocket-level PING/PONG keepalive runs every 20s with a 10s pong timeout (`websocket-client`'s `WebSocketApp.run_forever(ping_interval, ping_timeout)`). A stalled or dead connection is detected within ~30s and triggers a reconnect from the last seen `time_us` cursor.
