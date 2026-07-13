# example_atproto_plugins

A sample Osprey plugin that consumes ATProto's [JetStream](https://docs.bsky.app/blog/jetstream) as the input event source. It gives you:

- a `register_input_stream` hook implementation that subscribes to JetStream over WebSocket and yields Osprey `Action`s with the JetStream JSON event passed through as-is,
- realistic per-second event volume from the live Bluesky network, which is useful for load and soak testing changes that the synthetic 1-event/second producer doesn't exercise,
- a companion `example_atproto_rules/` tree showing how to organize rules against ATProto event shapes, with file structure modeled on [haileyok/atproto-ruleset](https://github.com/haileyok/atproto-ruleset).

This package registers the **input stream** plus two optional enrichment UDFs (see below). The sample rules also use a UDF (`TextContains`), a labels service, and an output sink that are provided by the sibling `example_plugins/` package, so the two run together: the worker image installs both, and Osprey loads every registered plugin, so `example_plugins` supplies those pieces automatically in the docker stack. If you lift this sample into a setup without `example_plugins`, provide those yourself (a labels provider and output sink) or restrict the rules to stdlib UDFs.

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
  "identity": {"did": "...", "seq": ..., "time": "..."}
}
```

JetStream identity events carry only `did` / `seq` / `time` — not the handle. Resolve the handle from the DID via the opt-in enrichment below.

Account events, commits for collections not in `COLLECTION_NAMES`, and commits with operations other than `create` / `update` / `delete` are skipped.

### Profile enrichment (opt-in)

JetStream events identify the actor only by DID, which isn't searchable the way a handle or display name is. The plugin ships two UDFs, `AtprotoHandle` and `AtprotoDisplayName`, that resolve a DID to those fields via Bluesky's public, unauthenticated AppView (`app.bsky.actor.getProfile`). Results are cached per DID, and lookups fail soft (the feature is simply absent) when the API errors or rate-limits.

**It is off by default.** Each unique DID costs an external API call, which is fine for a demo but is exactly the kind of dependency you don't want in a load test — so the default rules run against the raw firehose with no outbound calls. To turn enrichment on:

1. Import `models/enrichment.sml` in `example_atproto_rules/main.sml`. Imports must stay lexicographically sorted, so the list becomes:

   ```
   Import(
     rules=[
       'models/base.sml',
       'models/enrichment.sml',
       'models/record/base.sml',
       'models/record/post.sml',
     ],
   )
   ```

2. Add `Handle` and `DisplayName` to the `['*']` feature list in `example_atproto_rules/config/ui_config.yaml` so they show in the event stream.

For a smoother demo once enabled, narrow `OSPREY_JETSTREAM_WANTED_COLLECTIONS` to lower the unique-DID (and thus request) rate.

### Extending the enrichment

`getProfile` returns the whole profile, and `enrichment_udfs.py` already caches it per DID, so more trust & safety signals are cheap to add — a new UDF just reads another field off the same cached fetch. For example, an account-age signal:

```python
from datetime import datetime, timezone


class AtprotoAccountAgeDays(UDFBase[DidArguments, int]):
    """Whole days since the account's profile was created."""

    category = _ATPROTO_CATEGORY
    execute_async = True

    def execute(self, execution_context: ExecutionContext, arguments: DidArguments) -> int:
        created_at = _profile_or_skip(arguments.did).get('createdAt')
        if not isinstance(created_at, str):
            raise ExpectedUdfException()
        created = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
        return max(0, (datetime.now(timezone.utc) - created).days)
```

Register it in `register_plugins.py`'s `register_udfs`, then reference it from `enrichment.sml`. The same pattern exposes `followersCount` / `followsCount` / `postsCount` (bot/spam heuristics), `description` (a scannable bio), or `labels` (moderation labels already applied to the account).

### UI default features

`example_atproto_rules/config/ui_config.yaml` declares the per-action default features the Osprey UI surfaces in the event stream — e.g. `UserId` for every action, `PostText` for `create_post`, `Subject` for like / repost / follow events. Add new entries there to expose more fields without touching rule code.

`action_id` is minted from `snowflake-id-worker` in batches of 250. The plugin therefore needs `SNOWFLAKE_API_ENDPOINT` to be set (the local docker-compose stack provides it).

## Caveats

- **Not production-ready.** No durable cursor on process restart, no zstd compression, no DID-level filtering. Good for sample / load-testing purposes; not a drop-in for a real ATProto deployment.
- **Enrichment is off by default and best-effort.** JetStream carries no handle/profile/account-age data. The opt-in `Handle` / `DisplayName` UDFs resolve a DID against the public AppView on demand (cached, fail-soft), which is enough for demos but will rate-limit at full firehose volume — so it stays off unless you enable it, keeping load tests dependency-free. Rulesets that need reliable, complete enrichment (such as much of [atproto-ruleset](https://github.com/haileyok/atproto-ruleset)) still want a dedicated enrichment pipeline in front of this one rather than per-event API lookups.
- **Connection health.** WebSocket-level PING/PONG keepalive runs every 20s with a 10s pong timeout (`websocket-client`'s `WebSocketApp.run_forever(ping_interval, ping_timeout)`). A stalled or dead connection is detected within ~30s and triggers a reconnect from the last seen `time_us` cursor.
