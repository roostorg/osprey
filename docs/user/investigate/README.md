# Investigate

The Investigate section is where most day-to-day work happens. It contains the main query interface, your query history, and saved queries.

## Query

The Query page is the home page of Osprey. It's a live investigation workspace with three panels.

![The Query page on first load: an empty query input with a Last Day time range on the left above the query history, and empty Timeseries Chart, Top N Results, and Event Stream panels awaiting data](../../images/osprey-home.png)

### Query input

The left panel is where you write and run queries. Osprey uses SML syntax—the same language used to write rules—to filter and search event data. See [Query Syntax](query-syntax.md) for a full reference.

![The query input with an SML filter for post-creation events by one user, a Last 2 Weeks time range, and a Submit Query button](../../images/query-box.png)

As you type, the input offers autocomplete suggestions for feature names, action names, and UDFs. Hovering over any UDF name shows a tooltip with its description.

![Autocomplete in the query input suggesting the ContainsHello UDF after typing "Contains", with a tooltip describing it as "Post contains the word hello"](../../images/query-udf-hover.png)

The active query is reflected in the page URL, making it easy to share a specific investigation with a teammate, though be aware this may expose sensitive query parameters.

### Time range

Every query runs against a time window. You can choose a preset interval—from the last second up to the last three months—or set a custom date range using the date picker.

![The time range dropdown under the query input, open to presets ranging from Last 12 Hours to Last 3 Months](../../images/query-time-range.png)

The entire page updates dynamically when the query or time range changes, and interacting with any other panel (charts, event stream) can also update the query in turn.

### Charts

The center panels show two types of visualizations:

![A Timeseries Chart of daily matching event counts above a Top N Results table grouping events by PostText, with Download CSV, Bulk Label, PoP, Yeet Table, and Precision controls along the table footer](../../images/charts.png)

**Timeseries** displays how many matching events occurred over time. You can set the granularity—minute, fifteen minutes, half hour, hour, day, week, or month—and hover over individual bars to see the count for that period.

![Hovering over a timeseries bar, showing a tooltip with that bucket's date, time, and event count](../../images/hover-time-series.png)

You can add additional timeseries charts to compare different time granularities side by side. Charts you no longer need can be removed with the **Yeet** button.

![Two timeseries charts for the same query stacked vertically: daily granularity above, per-minute granularity below, with a Yeet button beside the second chart](../../images/multiple-time-series.png)

**Top N** shows a table of the top results for the current query, grouped by a dimension you choose. You can:
- Add or remove dimension columns
- Adjust the number of results shown with **Precision**
- Toggle **PoP** (period over period) to compare current results against a past time window and see the delta
- Export the table as a CSV
- Remove the table with the **Yeet Table** button

![A Top N Results table grouping matching events by UserId, sorted by count](../../images/top-n-charts.png)

![A Top N Results table with PoP enabled, adding delta and percent-change columns comparing each count against the previous period, with values redacted](../../images/pop.png)

### Event stream

The right panel is Osprey's live feed. It shows individual events matching the current query in near real time, and can also be used to search historical events.

![The Event Stream filtered to one UserId, showing payment events as cards listing each extracted feature, with sensitive values redacted](../../images/event-stream.png)

The stream can be displayed in card format or list format. On first load, each event card shows all of its extracted features (or the summary features configured for that event type, if any), so the stream is useful before you've set anything up. You can customize which fields appear per event with **Select Summary Features**, helpful when different team members care about different metadata.

![The Set Features modal for the event stream, with a toggle for Set Custom Features and a filterable checklist of features grouped by model and rule](../../images/summary-features.png)

Selecting an entity in the event stream (such as a user ID or IP address) opens the [Entity Details](labels.md#entity-details) view. Hold <kbd>Ctrl</kbd> (<kbd>⌘ Cmd</kbd> on macOS) while clicking to select multiple events for bulk labeling.

Selecting an event opens a detail view at `/events/:eventId` showing all extracted feature values for that specific event.

## Query History

Every query you run is automatically saved to your history.

![A history entry highlighted in the sidebar's History tab, showing its query text and original time range beside the results it produced](../../images/query-history.png)

Hovering over a query in the sidebar shows the Top N dimensions that were active during that session.

The full Query History page (accessible from the sidebar) shows a searchable list of all queries run across your team. You can filter by user email, view the original query text, and re-run any past query using the same time range it was originally run with.

![The Query History page listing each query with its text and time range, buttons to view the original query or re-run it, a filter-by-user dropdown, and a Saved Queries sidebar](../../images/query-history-page.png)

## Saved Queries

For queries you return to frequently, Osprey lets you save them by name.

![The menu on a query history entry, with View Original Query and Save Query options](../../images/query-history-save.png)

The Saved Queries page (accessible from the sidebar) lists all saved queries with the query text, the user who saved it, and when it was saved. You can filter by user email.

![The Saved Queries page listing a saved query with its text and metadata, and an open menu of Run Original Query, Run Query Using Interval, Show Saved Query History, and Delete Saved Query options](../../images/saved-queries-page.png)

From each saved query's menu, you can:
- **Run Original Query** to re-run it over its original time range
- **Run Query Using Interval** to run it over its saved interval (e.g. Last Day) relative to now
- **Show Saved Query History** to see past runs of the query
- **Delete Saved Query**, with a confirmation step

To rename a saved query, run it and select the edit icon next to its name on the Query page.

Saved queries also have a direct URL: `/saved-query/:savedQueryId/latest` automatically loads and executes the query.
