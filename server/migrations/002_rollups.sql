-- PulseWatch Rollups: Mathematically Correct Continuous Aggregates
-- Stores sum, count, min, and max to prevent "averages of averages" errors.

-- 1. Continuous Aggregate View: 1-Minute Rollups
CREATE MATERIALIZED VIEW IF NOT EXISTS metrics_rollup_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', ts) AS bucket,
    host_id,
    metric_id,
    SUM(value) AS sum_val,
    COUNT(value) AS count_val,
    MIN(value) AS min_val,
    MAX(value) AS max_val
FROM metrics
GROUP BY bucket, host_id, metric_id
WITH NO DATA;

-- Refresh policy: refresh recent 1-minute buckets every 1 minute
SELECT add_continuous_aggregate_policy('metrics_rollup_1m',
    start_offset => INTERVAL '2 hours',
    end_offset   => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists => TRUE);

-- 2. Retention Policies
-- Drop raw high-frequency metrics older than 7 days
SELECT add_retention_policy('metrics', INTERVAL '7 days', if_not_exists => TRUE);

-- Drop 1-minute rollups older than 30 days
SELECT add_retention_policy('metrics_rollup_1m', INTERVAL '30 days', if_not_exists => TRUE);
