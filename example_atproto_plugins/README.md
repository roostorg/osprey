# example_atproto_plugins

A sample Osprey plugin that consumes Bluesky's [JetStream](https://docs.bsky.app/blog/jetstream) — the public ATProto firehose — as the input event source.

This is the missing companion to `example_plugins/`: it shows how a real-world adopter wires Osprey up to their own platform's events. Out of the box it gives you:

- a `register_input_stream` hook implementation that subscribes to JetStream over WebSocket and yields Osprey `Action`s with the JetStream JSON event passed through as-is,
- realistic per-second event volume from the live Bluesky network — useful for load and soak testing changes that the synthetic 1-event/second producer doesn't exercise,
- a companion `example_atproto_rules/` tree showing how to organize rules against ATProto event shapes, with file structure modeled on [haileyok/atproto-ruleset](https://github.com/haileyok/atproto-ruleset).

## Running

From the repo root:

```sh
./run-atproto.sh
```

This brings up the full Osprey local stack (Druid, Postgres, Bigtable, MinIO, Kafka) along with a JetStream WebSocket override, and swaps the worker's input source from Kafka to the JetStream plugin, pointing it at `example_atproto_rules` instead of `example_rules`. First-run startup takes a few minutes.

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

`example_atproto_rules/config/ui_config.yaml` declares the per-action default features the Osprey UI surfaces in the event stream — e.g. `PostText` for `create_post`, `IdentityHandle` for `identity`, `LikeSubjectUri` for like events. Add new entries there to expose more fields without touching rule code.

`action_id` is minted from `snowflake-id-worker` in batches of 250. The plugin therefore needs `SNOWFLAKE_API_ENDPOINT` to be set (the local docker-compose stack provides it).

## Caveats

- **Not production-ready.** No durable cursor on process restart, no zstd compression, no DID-level filtering. Good for sample / load-testing purposes; not a drop-in for a Bluesky deployment. In-process reconnect resumes from last seen event via socket-level cursor.
- **No event enrichment.** JetStream only carries what's in the commit itself; rulesets that depend on handle / profile / account age (such as much of [atproto-ruleset](https://github.com/haileyok/atproto-ruleset)) are fed by a separate enrichment pipeline, not JetStream directly. This plugin emits JetStream-native paths ($.did, $.commit.collection, etc.); enrichment-fed rulesets would need an enrichment service in front of this one or a different plugin.
- **No application-level keepalive.** Connection health is monitored via socket read timeout (30s). A stalled connection will be detected and trigger reconnection.
- **Schema drift.** Bluesky has not committed to JetStream as a stable long-term API. Treat this as illustrative.
