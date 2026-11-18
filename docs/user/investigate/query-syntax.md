# Query Syntax

Osprey uses SML (“Some Madeup Language,” a subset of Python with additional restrictions) for queries. Queries match against **features**—the named values your rules extract from each event—including **entities** and **labels**. If those terms are new, read [Concepts](../../concepts.md) first.

A query can only use features your rules define, compared with values of the feature's type. The examples below use the features of the demo ruleset in `example_rules/`: `EventType`, `UserId` and `PostText` are strings, and the demo's users are labeled `meow`. Substitute your own deployment's features; the [Features Registry](../manage.md#features-registry) lists them with their types. A feature that doesn't exist is rejected with "does not exist in any rule files", and comparing a feature with a value of the wrong type (`UserId == 4454` for a string feature) with "incompatible types".

## Basic comparisons

```py
EventType == "create_post"
UserId == "user_4454"
PostText != None
```

## Combining conditions

```py
# AND: all conditions must match
EventType == "create_post" and UserId == "user_4454"

# OR: any condition may match
(UserId == "user_4454") or (UserId == "user_8284")

# in: match any value in a list
EventType in ["create_post", "update_post"]
```

## Query functions

Queries support a small, fixed set of built-in functions. Unlike the UDFs you use in rules (which are pluggable), this set does not grow when you add plugins:

- `RegexMatch(target=..., pattern=...)`: regex match against a feature
- `DidAddLabel(...)` and `DidRemoveLabel(...)`: events where a label was added or removed (see [Label queries](#label-queries))
- `DidDeclareVerdict(...)`: events where a verdict was declared

```py
# Regex match against a feature
RegexMatch(target=PostText, pattern="(buy|sell|deal)")
```

> [!NOTE]
> These are the only functions that work in queries. The [UDF Registry](../manage.md#udf-registry) lists every UDF, but most are rules-only: using one in a query is rejected with an `unknown function` error, shown under the query box.

## Label queries

The query UI searches across events, not entity state, so `HasLabel()` won't work here. Use `DidAddLabel()` instead, which matches events where a label was added:

```py
# Find events that added a specific label
DidAddLabel(entity_type="User", label_name="meow")
```

## Example queries

```py
# One user's posts
EventType == "create_post" and UserId == "user_4454"

# Posts matching a pattern
EventType == "create_post" and RegexMatch(target=PostText, pattern="urgent")

# Events where a user was labeled meow
DidAddLabel(entity_type="User", label_name="meow")

# Complex: posts matching a pattern that didn't label their author
EventType == "create_post" and
RegexMatch(target=PostText, pattern="(click|link|urgent)") and
not DidAddLabel(entity_type="User", label_name="meow")
```
