-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Interned metric definitions (prevents storing raw string on every row)
CREATE TABLE IF NOT EXISTS metric_definitions (
    id SERIAL PRIMARY KEY,
    name VARCHAR(128) NOT NULL UNIQUE
);

-- Interned label sets (normalized key-value dimensions)
CREATE TABLE IF NOT EXISTS label_sets (
    id BIGSERIAL PRIMARY KEY,
    labels_hash BIGINT NOT NULL UNIQUE,
    labels JSONB NOT NULL DEFAULT '{}'::jsonb
);

-- Registered hosts & liveness tracking
CREATE TABLE IF NOT EXISTS hosts (
    host_id VARCHAR(64) PRIMARY KEY,
    hostname VARCHAR(255) NOT NULL DEFAULT '',
    os VARCHAR(64) NOT NULL DEFAULT '',
    arch VARCHAR(64) NOT NULL DEFAULT '',
    agent_version VARCHAR(64) NOT NULL DEFAULT '',
    first_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status VARCHAR(32) NOT NULL DEFAULT 'online'
);

-- Per-host sequence tracking for crash-resilient deduplication (idempotency)
CREATE TABLE IF NOT EXISTS agent_sequences (
    host_id VARCHAR(64) PRIMARY KEY,
    last_acked_seq BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Hot time-series metrics table
CREATE TABLE IF NOT EXISTS metrics (
    ts TIMESTAMPTZ NOT NULL,
    host_id VARCHAR(64) NOT NULL,
    metric_id INT NOT NULL REFERENCES metric_definitions(id),
    label_set_id BIGINT NOT NULL REFERENCES label_sets(id),
    value DOUBLE PRECISION NOT NULL,
    CONSTRAINT pk_metrics PRIMARY KEY (host_id, metric_id, label_set_id, ts)
);

-- Convert metrics to TimescaleDB Hypertable partitioned by timestamp (1-day chunks)
SELECT create_hypertable('metrics', by_range('ts', INTERVAL '1 day'), if_not_exists => TRUE);

-- Primary query index for metric lookups by host and time range
CREATE INDEX IF NOT EXISTS idx_metrics_host_metric_ts ON metrics (host_id, metric_id, ts DESC);
