-- =========================================================
-- Bootstrap script for streaming_system database.
-- Runs automatically on first Postgres init via
-- docker-entrypoint-initdb.d, or manually for existing DBs.
--
-- Idempotent: safe to re-run (IF NOT EXISTS / DO $$ blocks).
-- =========================================================

-- =========================================================
-- 1) Schemas
-- =========================================================
CREATE SCHEMA IF NOT EXISTS ingest;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS monitoring;

-- =========================================================
-- 2) Core tables
-- =========================================================
CREATE TABLE IF NOT EXISTS public.raw_prices (
    event_id         TEXT PRIMARY KEY,
    commodity        TEXT NOT NULL,
    symbol           TEXT NOT NULL,
    price            DOUBLE PRECISION NOT NULL,
    currency         TEXT NOT NULL,
    event_ts         TIMESTAMP NOT NULL,
    source           TEXT,
    ingest_ts        TIMESTAMP,
    kafka_partition  INTEGER,
    kafka_offset     BIGINT
);

-- =========================================================
-- 3) Monitoring tables
-- =========================================================
CREATE TABLE IF NOT EXISTS monitoring.api_calls (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ts_utc       TIMESTAMPTZ NOT NULL DEFAULT now(),
    symbols      TEXT,
    http_status  INTEGER,
    latency_ms   INTEGER,
    ok           BOOLEAN NOT NULL,
    error_type   TEXT,
    error_msg    TEXT
);

CREATE TABLE IF NOT EXISTS monitoring.dead_letter_events (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ts_utc              TIMESTAMPTZ NOT NULL DEFAULT now(),
    stream_instance_id  TEXT,
    batch_id            INTEGER,
    topic               TEXT,
    kafka_partition     INTEGER,
    kafka_offset        BIGINT,
    error_reason        TEXT,
    raw_payload         TEXT
);

CREATE TABLE IF NOT EXISTS monitoring.kafka_lag (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    group_id            TEXT NOT NULL,
    topic               TEXT NOT NULL,
    ts_utc              TIMESTAMPTZ NOT NULL DEFAULT now(),
    total_lag           BIGINT,
    max_partition_lag   BIGINT
);

CREATE TABLE IF NOT EXISTS monitoring.alert_events (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ts_utc          TIMESTAMPTZ NOT NULL DEFAULT now(),
    source          TEXT,
    severity        TEXT,
    alert_uid       TEXT,
    alert_title     TEXT,
    state           TEXT,
    dashboard_uid   TEXT,
    panel_id        INTEGER,
    org_id          INTEGER,
    raw_payload     JSONB
);

CREATE TABLE IF NOT EXISTS monitoring.dbt_test_runs (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ts_utc      TIMESTAMPTZ NOT NULL DEFAULT now(),
    environment TEXT,
    status      TEXT,
    total       INTEGER,
    pass        INTEGER,
    warn        INTEGER,
    error       INTEGER,
    fail        INTEGER,
    skipped     INTEGER
);

CREATE TABLE IF NOT EXISTS monitoring.backup_log (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ts_utc          TIMESTAMPTZ NOT NULL DEFAULT now(),
    file_name       TEXT,
    file_size_bytes BIGINT,
    status          TEXT,
    error_msg       TEXT
);

-- =========================================================
-- 4) Monitoring views (used by Grafana alerts & dashboards)
-- =========================================================

-- Pipeline health metrics (single-row summary for alert rules)
CREATE OR REPLACE VIEW monitoring.pipeline_metrics AS
SELECT
    EXTRACT(EPOCH FROM (now() - MAX(ingest_ts)))::integer
        AS time_since_last_ingest_seconds,
    COUNT(*) FILTER (WHERE ingest_ts >= now() - interval '6 minutes')
        AS events_last_6m,
    COUNT(*) FILTER (WHERE ingest_ts >= now() - interval '15 minutes' AND commodity = 'bitcoin')
        AS btc_events_last_15m
FROM public.raw_prices;

-- API error metrics over the last 18 minutes (alert threshold window)
CREATE OR REPLACE VIEW monitoring.api_metrics_18m AS
SELECT
    COUNT(*) AS calls_18m,
    COUNT(*) FILTER (WHERE NOT ok) AS errors_18m,
    CASE WHEN COUNT(*) > 0
        THEN ROUND(100.0 * COUNT(*) FILTER (WHERE ok) / COUNT(*), 1)
        ELSE 100.0
    END AS success_rate_18m
FROM monitoring.api_calls
WHERE ts_utc >= now() - interval '18 minutes';

-- Latest Kafka lag per consumer group + topic (avoids scanning full history)
CREATE OR REPLACE VIEW monitoring.kafka_lag_latest AS
SELECT DISTINCT ON (group_id, topic)
    group_id,
    topic,
    ts_utc,
    total_lag,
    max_partition_lag
FROM monitoring.kafka_lag
ORDER BY group_id, topic, ts_utc DESC;
