# User Guide

Osprey is a web-based investigation and management console for safety teams. Query event data in real time, visualize trends, label entities, manage rules and features, and run bulk operations.

[![The Osprey UI during an investigation: a query filtering events where SuspiciousDisplayName is True, timeseries charts of matching event volume, and the live event stream with one entity's label popup open showing a negative spam_display_name label](../images/osprey-suspicious-display-name.png)](../images/osprey-suspicious-display-name.png)

The sidebar groups Osprey's tools by task, and this guide follows the same sections:

- **[Investigate](investigate/)**: query events in real time, chart the results, and drill into individual events and entities. Query history and saved queries let you revisit and share past investigations.

  [![Two Top N tables for a query, grouping matching events by event type and by post text](../images/multiple-top-charts.png)](../images/multiple-top-charts.png)

- **[Manage](manage.md)**: browse the rules, features, and UDFs configured in your deployment, and visualize how rules and labels relate.

  [![The UDF Registry listing available functions with type signatures and descriptions, grouped by category](../images/udf-documentation.png)](../images/udf-documentation.png)

- **[Operate](operate.md)**: run bulk labeling jobs over query results and review past jobs.

  [![The Bulk Edit Labels form for roughly 9,800 entities matching a query, with label name, status, reason, and expiration fields](../images/bulk-label.png)](../images/bulk-label.png)

The sidebar can be collapsed to an icon-only strip with the toggle at the bottom, and its state persists between sessions. The interface supports light and dark themes; see [Appearance](appearance.md) for details.
