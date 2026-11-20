# Ruleset Configuration

SML files define features, rules, and effects. A ruleset can also include YAML configuration for behavior that sits outside the SML execution graph, such as declaring labels or choosing what the UI displays.

Configuration doesn't create SML variables and can't replace rule logic. Engine and worker components read each configuration section for their own purposes.

## File layout

Configuration is optional unless your rules use labels, which must be declared in a `labels` section: a ruleset with no YAML at all validates and runs, with every section at its defaults. When you do add configuration, put it in `config.yaml` at the ruleset root or in files with a lowercase `.yaml` extension directly inside a directory named `config`. Osprey scans every directory named `config` in the ruleset tree and loads the `.yaml` files immediately inside it. Keep exactly one `config/` directory at the root to make file discovery predictable.

```text
my-rules/
|-- main.sml
|-- config.yaml
|-- config/
|   |-- labels.yaml
|   `-- ui_config.yaml
|-- models/
`-- rules/
```

Don't leave a discovered configuration file empty (use `{}` if it has no settings yet): if any file is empty, Osprey treats the entire merged configuration as empty, and publishing the ruleset can fail.

Osprey deep-merges the files into one configuration object. Mappings combine and lists concatenate. Defining the same scalar key in more than one file causes ruleset loading to fail, even when the values match. Keeping each top-level section in one file avoids surprising merges.

## Configuration sections

Each top-level YAML key is a configuration section. Osprey registers a schema for every section it understands, then checks the merged configuration alongside SML source validation and before execution graph compilation. Unsupported sections and invalid values prevent the ruleset from activating.

For example, the demo ruleset declares the `meow` label used by its SML rules:

```yaml
labels:
  meow:
    valid_for: [User]
    connotation: positive
    description: testing label
```

This configuration lets Osprey:

- reject a rule that uses an undeclared label or applies it to the wrong entity type
- show the label's description and connotation in the UI
- give `LabelAdd`, `LabelRemove`, and `HasLabel` a shared label definition

Other sections configure worker or UI behavior. The available sections depend on the worker build in your deployment; a top-level key is valid only when that worker has registered it.

### `ui_config`

Controls what the UI shows by default; the UI reads it from the UI API's `/config` endpoint.

```yaml
ui_config:
  # Features shown on Event Stream cards before a user picks their own summary features.
  # Each entry lists action names ('*' for all) and the features to show for them.
  default_summary_features:
    - actions: ['*']
      features: [UserId]
    - actions: ['create_post']
      features: [PostText]
  # Per entity type, a URL for the entity's "More Info" button; {entity_id} is replaced
  # with the URL-encoded entity ID.
  external_links:
    User: https://admin.example.com/users/{entity_id}
```

Both keys are optional. [`example_atproto_rules/config/ui_config.yaml`](https://github.com/roostorg/osprey/blob/main/example_atproto_rules/config/ui_config.yaml) is a fuller example.

### `actions`

Per action name, a `sample_rate` from 0 to 100 that tells the worker to drop some events of that action before running rules. The default, `100`, processes every event, and `0` drops every event of that action. Any other value is the percentage of events dropped: `sample_rate: 10` drops about 10% and processes about 90%.

```yaml
actions:
  create_post:
    sample_rate: 100
```

## Applying updates

Configuration is versioned with its SML sources and contributes to the ruleset hash. When a source provider delivers an updated ruleset, Osprey validates the YAML and SML, then compiles the execution graph before activating it. If validation or compilation fails, the update doesn't go live.

A directory-backed worker reads the ruleset at startup; it doesn't watch files for changes. Use [`osprey-cli push-rules`](../development/cli-reference.md#osprey-cli-push-rules) when your deployment accepts pushed rules: it publishes them to etcd, so it needs an etcd endpoint. `push-rules --dry-run` validates a ruleset locally without one.

See [Examples](examples.md) for a complete ruleset that uses `config/labels.yaml` alongside SML rules.
