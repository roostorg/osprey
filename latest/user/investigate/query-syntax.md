# Query Syntax

Osprey uses SML (“Some Madeup Language,” a subset of Python with additional restrictions) for queries. Queries match against **features**—the named values your rules extract from each event—including **entities** and **labels**. If those terms are new, read [Concepts](../../concepts.md) first.

## Basic comparisons

```py
EventType == "create_post"
UserId == 12345
MessageText != None
```

## Combining conditions

```py
# AND: all conditions must match
EventType == "user_login" and LoginAttempts >= 3

# OR: any condition may match
(UserId == 123) or (UserId == 456)

# in: match any value in a list
EventType in ["create_post", "send_message"]
```

## Query functions

Queries support a small, fixed set of built-in functions. Unlike the UDFs you use in rules (which are pluggable), this set does not grow when you add plugins:

- `RegexMatch(target=..., pattern=...)`: regex match against a feature
- `DidAddLabel(...)` and `DidRemoveLabel(...)`: events where a label was added or removed (see [Label queries](#label-queries))
- `DidDeclareVerdict(...)`: events where a verdict was declared

```py
# Regex match against a feature
RegexMatch(target=MessageText, pattern="(buy|sell|deal)")
```

> [!NOTE]
> These are the only functions that work in queries. The [UDF Registry](../manage.md#udf-registry) lists every UDF, but most are rules-only and will fail with a silent 500 error if you use them in a query.

## Label queries

The query UI searches across events, not entity state, so `HasLabel()` won't work here. Use `DidAddLabel()` instead, which matches events where a label was added:

```py
# Find events that added a specific label
DidAddLabel(entity_type="User", label_name="likely_spammer")
DidAddLabel(entity_type="IpAddress", label_name="suspicious")
```

## Example queries

```py
# Suspicious login attempts
EventType == "user_login" and LoginAttempts >= 5

# Posts matching a pattern
EventType == "create_post" and RegexMatch(target=PostContent, pattern="urgent")

# Users who were flagged
DidAddLabel(entity_type="User", label_name="flagged")

# Complex: messages matching a pattern, from users without a verified label
EventType == "send_message" and
RegexMatch(target=MessageText, pattern="(click|link|urgent)") and
not DidAddLabel(entity_type="User", label_name="verified")
```
