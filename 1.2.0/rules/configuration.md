# Ruleset Configuration

SML files define features, rules, and effects. A ruleset can also include YAML configuration for behavior that sits outside the SML execution graph, such as declaring labels or choosing what the UI displays.

Configuration doesn't create SML variables and can't replace rule logic. Engine and worker components read each configuration section for their own purposes.

## File layout

A directory-backed ruleset needs at least one configuration source: `config.yaml` at the ruleset root or a file with a lowercase `.yaml` extension directly inside a directory named `config`. Osprey scans every directory named `config` in the ruleset tree and loads the `.yaml` files immediately inside it. Keep exactly one `config/` directory at the root to make file discovery predictable.

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

Use `{}` in `config.yaml` when the ruleset has no settings. Don't leave a discovered configuration file empty: if any file is empty, Osprey treats the entire merged configuration as empty, and publishing the ruleset can fail.

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

Other sections configure worker or UI behavior. For example, `ui_config` selects default summary features and external links. The available sections depend on the worker build in your deployment; a top-level key is valid only when that worker has registered it.

## Applying updates

Configuration is versioned with its SML sources and contributes to the ruleset hash. When a source provider delivers an updated ruleset, Osprey validates the YAML and SML, then compiles the execution graph before activating it. If validation or compilation fails, the update doesn't go live.

A directory-backed worker reads the ruleset at startup; it doesn't watch files for changes. Use [`osprey-cli push-rules`](../development/cli-reference.md#osprey-cli-push-rules) when your deployment accepts pushed rules.

See [Examples](examples.md) for a complete ruleset that uses `config/labels.yaml` alongside SML rules.
