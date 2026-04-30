# example_atproto_plugins

A sample Osprey plugin that consumes Bluesky's [JetStream](https://docs.bsky.app/blog/jetstream) — the public ATProto firehose — as the input event source.

This is the missing companion to `example_plugins/`: it shows how a real-world adopter wires Osprey up to their own platform's events. Out of the box it gives you:

- a `register_input_stream` hook implementation that subscribes to JetStream over WebSocket and yields Osprey `Action`s for ATProto commits (posts, likes, reposts, follows by default),
- realistic per-second event volume from the live Bluesky network — useful for load and soak testing changes that the synthetic 1-event/second producer doesn't exercise,
- a companion `example_atproto_rules/` tree showing how to write rules against ATProto event shapes.

## Running

From the repo root:

```sh
./run-atproto.sh
```

Under the hood this stacks `docker-compose.atproto.yaml` on top of the main compose file, swapping the worker's input source from Kafka to the JetStream plugin and pointing it at `example_atproto_rules` instead of `example_rules`.

## Configuration

| Env var | Default | Description |
| --- | --- | --- |
| `OSPREY_INPUT_STREAM_SOURCE` | (must be) `plugin` | Selects the plugin-provided stream. |
| `OSPREY_JETSTREAM_ENDPOINT` | `wss://jetstream2.us-west.bsky.network/subscribe` | JetStream WebSocket URL. |
| `OSPREY_JETSTREAM_WANTED_COLLECTIONS` | `app.bsky.feed.post,app.bsky.feed.like,app.bsky.feed.repost,app.bsky.graph.follow` | Comma-separated collections to subscribe to (server-side filter). |

## Action shape

Each commit is mapped to an `Action` with:

```
data = {
    'did': '<actor DID>',
    'collection': 'app.bsky.feed.post',
    'operation': 'create' | 'update' | 'delete',
    'rkey': '<record key>',
    'rev': '<commit rev>',
    'cid': '<record CID>',
    'event_type': 'create_post',         # <operation>_<short_collection>
    'record': { ... raw ATProto record ... },
}
```

Identity and account events are skipped — this sample is concerned with content events.

## Caveats

- **Not production-ready.** No durable cursor / resume on restart, no zstd compression, no DID-level filtering. Good for sample / load-testing purposes; not a drop-in for a Bluesky deployment.
- **Schema drift.** Bluesky has not committed to JetStream as a stable long-term API. Treat this as illustrative.
