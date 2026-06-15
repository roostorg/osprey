# Investigate

The Investigate section is where most day-to-day work happens. It contains the main query interface, your query history, and saved queries.

## Query

The Query page is the home page of Osprey. It's a live investigation workspace with three panels.

![Osprey Home](../images/osprey-home.png)

### Query input

The left panel is where you write and run queries. Osprey uses SML syntax — the same language used to write rules — to filter and search event data. See [Query Syntax](#query-syntax) below for a full reference.

![Query Box](../images/query-box.png)

As you type, the input offers autocomplete suggestions for feature names, action names, and UDFs. Hovering over any UDF name shows a tooltip with its description.

![Query UDF Hover](../images/query-udf-hover.png)

The active query is reflected in the page URL, making it easy to share a specific investigation with a teammate — though be aware this may expose sensitive query parameters.

### Time range

Every query runs against a time window. You can choose a preset interval — from the last second up to the last three months — or set a custom date range using the date picker.

![Query Time Range](../images/query-time-range.png)

The entire page updates dynamically when the query or time range changes, and interacting with any other panel (charts, event stream) can also update the query in turn.

### Charts

The center panels show two types of visualizations:

![Charts](../images/charts.png)

**Timeseries** displays how many matching events occurred over time. You can set the granularity — minute, fifteen minutes, half hour, hour, day, week, or month — and hover over individual bars to see the count for that period.

![Time Series Hover](../images/hover-time-series.png)

You can add additional timeseries charts to compare different time granularities side-by-side. Charts you no longer need can be removed ("yeeted").

![Multiple Time Series](../images/multiple-time-series.png)

**Top N** shows a table of the top results for the current query, grouped by a dimension you choose. You can:
- Add or remove dimension columns
- Adjust the number of results shown (precision)
- Enable **Period over Period (PoP)** to compare current results against a past time window and see the delta
- Export the table as a CSV

![Top N Charts](../images/top-n-charts.png)

![Period over Period](../images/pop.png)

### Event stream

The right panel is Osprey's live feed. It shows individual events matching the current query in near-real time, and can also be used to search historical events.

![Event Stream](../images/event-stream.png)

The stream can be displayed in card format or list format. You can customize which fields appear per event by clicking **Summary Features** — helpful when different team members care about different metadata.

![Summary Features](../images/summary-features.png)

Clicking an entity in the event stream (such as a user ID or IP address) opens the [Entity Details](#entity-details) view. Hold Ctrl (or Cmd on Mac) while clicking to select multiple events for bulk labeling.

Clicking an event opens a detail view at `/events/:eventId` showing all extracted feature values for that specific event.

---

## Query History

Every query you run is automatically saved to your history.

![Query History](../images/query-history.png)

Hovering over a query in the sidebar shows the Top N dimensions that were active during that session.

The full Query History page (accessible from the sidebar) shows a searchable list of all queries run across your team. You can filter by user email, view the original query text, and re-run any past query using the same time range it was originally run with.

![Query History Page](../images/query-history-page.png)

---

## Saved Queries

For queries you return to frequently, Osprey lets you save them by name.

![Save Query](../images/query-history-save.png)

The Saved Queries page (accessible from the sidebar) shows a grid of all saved queries with the query text, the user who saved it, and when it was saved. You can filter by user email.

![Saved Queries Page](../images/saved-queries-page.png)

From the grid, you can:
- **Run** a saved query to load it into the Query page
- **Rename** it via an edit modal
- **Delete** it (with a confirmation step)

Saved queries also have a direct URL: `/saved-query/:savedQueryId/latest` automatically loads and executes the query.

---

## Labeling

Labels are annotations you apply to entities — users, IP addresses, emails, and other tracked objects. They're the bridge between human judgment and Osprey's automated rule system: a label you apply manually can feed into rules that act on future events automatically.

Labels have three polarities:

- **Negative** — harmful or problematic (e.g. `spammer`, `bot`, `banned`, `suspicious`)
- **Positive** — trusted or verified (e.g. `verified`, `trusted`, `premium_user`)
- **Neutral** — informational (e.g. `new_user`, `from_mobile`, `beta_tester`)

A reason is required whenever you apply a label.

### Applying labels manually

From a Top N table, hover over an entity row and click **Edit Labels**:

![Add Labels](../images/add-labels.png)

From the event stream, click any entity to open its label drawer.

![Empty Label](../images/empty-label.png)

![Complete Label](../images/complete-label.png)

### Entity Details

Clicking an entity anywhere in the UI navigates to `/entity/:entityType/:entityId` — a deep-dive view showing every label that has ever been applied to that entity, grouped by label name.

![User Entity](../images/osprey-user-entity.png)

Each label entry shows:
- The label value and type
- Whether it was applied by a rule (automated), manually, or via a bulk action
- The reason provided
- Who applied it and when

---

## Query Syntax

Osprey uses SML (Structured Rule Logic) syntax for queries. Queries filter events by matching against features, actions, and labels.

### Core concepts

**Actions** are events — things that happen in your system, like a user creating a post or sending a message. Your query filters which action types to look at.

**Features** are named variables extracted from events. All features are in a global namespace; any feature exported by Osprey rules is queryable. Prefixing a variable with `_` keeps it local to a rule file and excludes it from querying.

```py
UserId: Entity[int] = EntityJson(type='User', path='$.user.id', coerce_type=True)
UserEmail: str = JsonData(type='Email', path='$.user.email', required=False)
```

Both `UserId` and `UserEmail` above are features.

**Entities** are a special kind of feature. They can have effects applied to them — labels, classifications, signals. Clicking an entity in the UI navigates to its [Entity Details](#entity-details) view.

**Effects** are outcomes triggered when one or more rules evaluate to true — for example, applying a `spammer` label to a `UserId` entity.

### Basic comparisons

```py
EventType == "create_post"
UserId == 12345
MessageText != Null
```

### Combining conditions

```py
# AND: all conditions must match
EventType == "user_login" and LoginAttempts >= 3

# OR: any condition may match
(UserId == 123) or (UserId == 456)

# in: match any value in a list
EventType in ["create_post", "send_message"]
```

### Using UDFs

UDFs (see [UDF Registry](manage.md#udf-registry)) extend what you can express in a query:

```py
# Text search
TextContains(text=PostContent, phrase="spam")
RegexMatch(target=MessageText, pattern="(buy|sell|deal)")

# List operations
ListLength(list=UserConnections) > 10
```

> **Note:** If you query a UDF that doesn't exist, Osprey will silently fail with a 500 error. Use the UDF Registry to confirm a function name before using it.

### Label queries

The query UI searches across actions (events), not entity state — so `HasLabel()` won't work here. Use `DidAddLabel()` instead, which matches events where a label was added:

```py
# Find events that added a specific label
DidAddLabel(entity_type="UserId", label_name="likely_spammer")
DidAddLabel(entity_type="IpAddress", label_name="suspicious")
```

### Example queries

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
