def array_contains_filter(column: str, value: str) -> dict[str, object]:
    """A Druid filter matching rows whose array (or multi-value string) `column` contains exactly `value`.

    Druid rejects pattern filters such as `like` and `selector` on ARRAY columns, so this uses
    `arrayContainsElement`. On its own that filter is "unknown" rather than false for rows where the column is
    null (no labels or verdicts), and Druid's three-valued logic then drops those rows from `not ...` as well.
    Requiring the column to be non-null makes the filter plain false for them, so negated queries match them.
    """
    return {
        'type': 'and',
        'fields': [
            {
                'type': 'arrayContainsElement',
                'column': column,
                'elementMatchValueType': 'STRING',
                'elementMatchValue': value,
            },
            {'type': 'not', 'field': {'type': 'null', 'column': column}},
        ],
    }
