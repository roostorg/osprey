# Query Syntax

Osprey uses SML (“Some Madeup Language,” a subset of Python with additional restrictions) for queries. Queries filter events by matching against features, actions, and labels.

## Core concepts

**Actions** are events that happen in your system, like a user creating a post or sending a message. Your query filters which action types to look at.

**Features** are named variables extracted from events. All features are in a global namespace; any feature exported by Osprey rules is queryable. Prefixing a variable with `_` keeps it local to a rule file and excludes it from querying.

```py
UserId: Entity[int] = EntityJson(type='User', path='$.user.id', coerce_type=True)
UserEmail: str = JsonData(type='Email', path='$.user.email', required=False)
```

Both `UserId` and `UserEmail` above are features.

**Entities** are a special kind of feature. They can have effects applied to them: labels, classifications, signals. Clicking an entity in the UI navigates to its [Entity Details](labels.md#entity-details) view.

**Effects** are outcomes triggered when one or more rules evaluate to true; for example, applying a `spammer` label to a `UserId` entity.

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

The query UI searches across actions (events), not entity state, so `HasLabel()` won't work here. Use `DidAddLabel()` instead, which matches events where a label was added:

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
