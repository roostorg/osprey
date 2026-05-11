CREATE DATABASE IF NOT EXISTS osprey;

CREATE TABLE IF NOT EXISTS osprey.execution_results_queue
(
    raw_features String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'osprey-kafka:29092',
    kafka_topic_list = 'osprey.execution_results',
    kafka_group_name = 'osprey-clickhouse-events',
    kafka_format = 'JSONAsString';

CREATE TABLE IF NOT EXISTS osprey.execution_results
(
    timestamp DateTime64(3, 'UTC'),
    action_id UInt64,
    action_name LowCardinality(String),
    raw_features String
)
ENGINE = MergeTree
ORDER BY (timestamp, action_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS osprey.execution_results_mv
TO osprey.execution_results
AS
SELECT
    coalesce(
        parseDateTime64BestEffortOrNull(JSONExtractString(raw_features, '__timestamp'), 3, 'UTC'),
        now64(3, 'UTC')
    ) AS timestamp,
    toUInt64(JSONExtractInt(raw_features, '__action_id')) AS action_id,
    JSONExtractString(raw_features, 'ActionName') AS action_name,
    raw_features
FROM osprey.execution_results_queue;
