# Examples

Everything [Writing Rules](./) covers, applied to complete rulesets: first the demo's real, runnable ruleset file by file, then two patterns you'll reach for as your own ruleset grows—labels as state, and a multi-signal rule organized across files. The examples use the same small social network as the rest of these docs.

## The demo ruleset, file by file

The [demo](../development/) runs the actual ruleset in [`example_rules/`](https://github.com/roostorg/osprey/tree/main/example_rules), which is small enough to read in one sitting: two models, one rule, and one label. The platform being moderated has a single (admittedly strict) policy: users may never say "hello."

Events arrive as JSON like this—an event name (`action_name`), an ID, and whatever the platform sent:

```json
{
  "action_id": 1,
  "action_name": "create_post",
  "data": {
    "user_id": "user_1923",
    "event_type": "create_post",
    "post": { "text": "hello world" }
  }
}
```

**`main.sml`** is the entrypoint. This ruleset is small enough to skip conditional `index.sml` files entirely: import the base model, require the one rule file.

```python
Import(rules=['models/base.sml'])

Require(rule='rules/post_contains_hello.sml')
```

**`models/base.sml`** defines the features present on every event. `UserId` and `EventType` are declared as entities, so Osprey tracks them as _things_ that persist across events—that's what lets a label stick to a user later. `coerce_type=True` (the default) converts a mismatched value, like a numeric user ID, to the declared type instead of erroring.

```python
UserId: Entity[str] = EntityJson(
  type='User',
  path='$.user_id',
  coerce_type=True
)

EventType: Entity[str] = EntityJson(
  type='EventType',
  path='$.event_type',
  coerce_type=True
)

ActionName=GetActionName()

ActionId=GetActionId()
```

The last two lines pull the event's name and ID from Osprey itself rather than the JSON payload: `GetActionName()` and `GetActionId()` are stdlib UDFs, and exposing them as features makes them queryable in the UI.

**`models/post.sml`** adds the one feature specific to posts:

```python
PostText: Entity[str] = EntityJson(
  type='PostText',
  path='$.post.text',
  coerce_type=True
)
```

**`rules/post_contains_hello.sml`** imports both models, defines the rule, and wires it to effects—the whole loop in one file:

```python
Import(
  rules=[
    'models/base.sml',
    'models/post.sml',
  ]
)

ContainsHello = Rule(
  when_all=[
    EventType == 'create_post',
    TextContains(text=PostText, phrase='hello')
  ],
  description='Post contains the word "hello"',
)

WhenRules(
  rules_any=[ContainsHello],
  then=[
    BanUser(entity=UserId, comment='User said "hello"'),
    LabelAdd(entity=UserId, label='meow'),
  ],
)
```

`TextContains` and `BanUser` aren't stdlib—they're custom UDFs from [`example_plugins/`](https://github.com/roostorg/osprey/tree/main/example_plugins), which is the reference for [writing your own](../integration/integrations.md#writing-udfs). `LabelAdd` is stdlib.

**`config/labels.yaml`** declares the label the rule applies: which entity types it's valid for, and its connotation.

```yaml
labels:
  meow:
    valid_for: [User]
    connotation: positive
    description: testing label
```

**What you'll see in the UI** once events flow: each processed post appears in the Event Stream with `UserId`, `EventType`, `PostText`, and `ContainsHello` among its extracted features; querying `ContainsHello == True` filters to the posts that matched; and clicking through to a matched post's author shows the `meow` label on their `User` entity, with the `BanUser` effect recorded on the event. [Getting Started](../development/) walks through this UI tour on live demo data.

## Labels as state

Rules can't see previous events directly, but labels persist on entities across events—so a label added by one rule becomes a condition another rule checks later. Say you've flagged users who send too many DMs:

```python
WhenRules(
    rules_any=[
        Sent_Too_Many_DMs,
    ],
    then=[
        LabelAdd(entity=UserId, label='likely_spammer')
    ],
)
```

From then on, _every_ event by that user carries the state, and other rules can act on it—here, on a different event type entirely:

```python
Should_Warn_User_Of_Spammer = Rule(
    when_all=[
        HasLabel(entity=UserId, label='likely_spammer'),
        This_Is_A_New_DM,
    ],
    description=f'Likely spammer {UserId} started a new DM',
)
```

Labels also show on the entity in the UI, where they can be added and removed by hand. One asymmetry to know about: `HasLabel()` works in rules but not in the query bar, because queries search events rather than current entity state. To find events where a label was applied, query `DidAddLabel(entity_type="UserId", label_name="likely_spammer")` instead; see [Query Syntax](../user/investigate/query-syntax.md) for details.

## A multi-signal rule, organized across files

The following is a complete walkthrough of writing a rule using the project structure described in [Writing Rules § Rule Structuring](./#rule-structuring). The goal is to flag accounts whose first post mentions at least one user and includes a link—three signals that are individually innocent but suspicious together.

### Writing the rule

We'll create `rules/record/post/first_post_link.sml` for the rule logic. This file defines both the conditions that cause the rule to evaluate to `True` and the actions to take when it does.

```python
# First, import the models that you will need inside of this rule
Import(
    rules=[
        'models/base.sml',
        'models/record/post.sml',
    ],
)

# Next, define a variable that uses the `Rule` UDF
FirstPostLinkRule = Rule(
    # Set the conditions in which this rule will be `True`
    when_all=[
        PostCount == 1, # if this is the user's first post
        EmbedLink != None, # if there is a link inside of the post
        ListLength(list=MentionIds) >= 1, # if there is at least one mention in the post
    ],
    description='First post for user includes a link embed',
)

# Finally, set which effect UDFs will be triggered
WhenRules(
    rules_any=[FirstPostLinkRule],
    then=[
        # This is a custom effect UDF that we have implemented
        ReportRecord(
            entity=PostId,
            comment='This was the first post by a user and included a link',
            severity=3,
        ),
    ],
)
```

### Wiring up the rule

We want this rule to run _only_ when the event is a post event. Using the project structure described above, this involves three files.

First, `main.sml` at the project root includes a single `Require` statement pointing to the top-level rules index:

```python
Require(
    rule='rules/index.sml',
)
```

Next, `rules/index.sml` conditionally requires the post rules when the event type matches:

```python
Import(
    rules=[
        'models/base.sml',
    ],
)

Require(
    rule='rules/record/post/index.sml',
    require_if=EventType == 'userPost',
)
```

Finally, `rules/record/post/index.sml` requires the new rule:

```python
Import(
    rules=[
        'models/base.sml',
        'models/record/post.sml',
    ],
)

Require(
    rule='rules/record/post/first_post_link.sml',
)
```

---

From here, try modifying the demo ruleset while it's running—add a phrase to `TextContains`, or a second rule over `PostText`—or head to [Integrations & Plugins](../integration/integrations.md) when a pattern needs a UDF that doesn't exist yet.
