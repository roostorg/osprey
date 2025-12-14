-- ClickHouse table for osprey execution results
-- As new features get added via rules, the ClickhouseExecutionResultsSink will
-- update the schema before performing batch inserts

CREATE TABLE IF NOT EXISTS default.osprey_execution_results (
    __action_id Int64,
    __timestamp DateTime64(3),
    __error_count Nullable(Int32)
) ENGINE = MergeTree()
ORDER BY (__action_id, __timestamp);
