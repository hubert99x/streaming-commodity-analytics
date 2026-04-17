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
-- Main fact table: one row per price observation.
-- event_id (UUID5) is deterministic — same (commodity, timestamp) always produces the same ID,
-- enabling idempotent inserts via ON CONFLICT (event_id) DO NOTHING.
CREATE TABLE IF NOT EXISTS public.raw_prices (
    event_id         TEXT PRIMARY KEY,        -- deterministic UUID5(commodity:timestamp)
    commodity        TEXT NOT NULL,            -- e.g. 'gold', 'bitcoin', 'eurusd'
    symbol           TEXT NOT NULL,            -- e.g. 'XAU/USD', 'BTC/USD', 'EUR/USD'
    price            DOUBLE PRECISION NOT NULL,
    currency         TEXT NOT NULL,            -- always 'USD' in current schema
    event_ts         TIMESTAMP NOT NULL,       -- when the price was observed at the source
    source           TEXT,                     -- e.g. 'twelvedata_rest'
    ingest_ts        TIMESTAMP,               -- when Spark wrote this row
    kafka_partition  INTEGER,                  -- for debugging and lag monitoring
    kafka_offset     BIGINT                   -- for debugging and lag monitoring
);

-- =========================================================
-- 3) Monitoring tables
-- =========================================================
-- Tracks every Twelve Data API call (success and failure) for observability.
-- Used by Grafana panels: API Calls, API Errors, API P95 Latency, API Success Rate.
CREATE TABLE IF NOT EXISTS monitoring.api_calls (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ts_utc       TIMESTAMPTZ NOT NULL DEFAULT now(),
    symbols      TEXT,          -- comma-separated symbols requested
    http_status  INTEGER,       -- NULL if request failed before HTTP response
    latency_ms   INTEGER,
    ok           BOOLEAN NOT NULL,
    error_type   TEXT,          -- e.g. 'RATE_LIMIT_429', 'SERVER_500', 'EXCEPTION'
    error_msg    TEXT
);

-- Dead Letter Queue: stores Kafka records that failed Spark validation.
-- Enables debugging bad data without losing the original payload.
CREATE TABLE IF NOT EXISTS monitoring.dead_letter_events (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ts_utc              TIMESTAMPTZ NOT NULL DEFAULT now(),
    stream_instance_id  TEXT,       -- identifies which Spark instance produced the error
    batch_id            INTEGER,    -- Spark micro-batch number
    topic               TEXT,
    kafka_partition     INTEGER,
    kafka_offset        BIGINT,     -- exact Kafka offset for tracing
    error_reason        TEXT,       -- e.g. 'JSON_PARSE_ERROR_OR_EMPTY', 'MISSING_FIELD:event_id'
    raw_payload         TEXT        -- original JSON string from Kafka
);

-- DLQ idempotent upsert constraint (prevents duplicate DLQ entries on batch replay).
-- Required by Spark's INSERT ... ON CONFLICT (stream_instance_id, batch_id, kafka_partition, kafka_offset).
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'uq_dlq_event'
    ) THEN
        ALTER TABLE monitoring.dead_letter_events
        ADD CONSTRAINT uq_dlq_event
        UNIQUE (stream_instance_id, batch_id, kafka_partition, kafka_offset);
    END IF;
END $$;

-- Kafka consumer lag snapshots, recorded every 60s by the kafka-lag monitor service.
-- Used by Grafana to detect if Spark is falling behind on processing.
CREATE TABLE IF NOT EXISTS monitoring.kafka_lag (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    group_id            TEXT NOT NULL,
    topic               TEXT NOT NULL,
    ts_utc              TIMESTAMPTZ NOT NULL DEFAULT now(),
    total_lag           BIGINT,
    max_partition_lag   BIGINT
);

-- Grafana alert history: every alert webhook payload is logged here.
-- Enables post-incident analysis without depending on Grafana's internal storage.
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

-- dbt test execution history: logged by run_dbt_test.sh after each test run.
-- Feeds the Grafana "dbt Test Runs" panel and "DBT Pass Rate" stat.
CREATE TABLE IF NOT EXISTS monitoring.dbt_test_runs (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_ts_utc  TIMESTAMPTZ NOT NULL DEFAULT now(),
    environment TEXT,
    status      TEXT,
    total       INTEGER,
    pass        INTEGER,
    warn        INTEGER,
    error       INTEGER,
    fail        INTEGER,
    skipped     INTEGER
);

-- Backup history: logged by backup-cron container after each pg_dump.
-- Feeds the Grafana "Backup Freshness" panel.
CREATE TABLE IF NOT EXISTS monitoring.backup_log (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    backup_ts       TIMESTAMPTZ NOT NULL DEFAULT now(),
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
    COUNT(*) FILTER (WHERE ingest_ts >= now() - interval '20 minutes' AND commodity = 'bitcoin')
        AS btc_events_last_20m
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
