# Query Syntax

Osprey uses SML (“Some Madeup Language,” a subset of Python with additional restrictions) for queries. Queries match against **features**—the named values your rules extract from each event—including **entities** and **labels**. If those terms are new, read [Concepts](../../concepts.md) first.

## Basic comparisons

```py
EventType == "create_post"
UserId == 12345
MessageText != Null
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

## Using UDFs

UDFs (see [UDF Registry](../manage.md#udf-registry)) extend what you can express in a query:

```py
# Text search
TextContains(text=PostContent, phrase="spam")
RegexMatch(target=MessageText, pattern="(buy|sell|deal)")

# List operations
ListLength(list=UserConnections) > 10
```

> [!NOTE]
> If you query a UDF that doesn't exist, Osprey will silently fail with a 500 error. Use the UDF Registry to confirm a function name before using it.

## Label queries

The query UI searches across events, not entity state, so `HasLabel()` won't work here. Use `DidAddLabel()` instead, which matches events where a label was added:

```py
# Find events that added a specific label
DidAddLabel(entity_type="UserId", label_name="likely_spammer")
DidAddLabel(entity_type="IpAddress", label_name="suspicious")
```

## Example queries

```py
# Suspicious login attempts
EventType == "user_login" and LoginAttempts >= 5

# Posts containing specific words
EventType == "create_post" and TextContains(text=PostContent, phrase="urgent")

# Users who were flagged
DidAddLabel(entity_type="UserId", label_name="flagged")

# Complex: messages matching a pattern, from users without a verified label
EventType == "send_message" and
RegexMatch(target=MessageText, pattern="(click|link|urgent)") and
not DidAddLabel(entity_type="UserId", label_name="verified")
```
