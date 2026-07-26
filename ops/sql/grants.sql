-- =========================================================
-- GRANTS for streaming_system
-- Roles are created by ops/sql/roles.sql, which runs first:
--   spark_writer, dbt_runner, grafana_read, producer_writer, backup_user,
--   alert_writer, lag_writer
-- Run as postgres superuser (automatically via docker-entrypoint-initdb.d).
-- =========================================================

-- 0) Schemas: ingest (Spark temp), analytics (dbt models), monitoring (ops metrics)
CREATE SCHEMA IF NOT EXISTS ingest;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS monitoring;

-- =========================================================
-- 1) Spark writer (Spark Structured Streaming)
-- Needs:
--   - create/overwrite staging tables in ingest
--   - insert/select into public.raw_prices (target)
--   - insert into monitoring.dead_letter_events (DLQ)
-- =========================================================

-- CREATE lets Spark build its two persistent staging tables on first start
-- (ingest.raw_prices_staging, ingest.dlq_staging). It owns them afterwards, so
-- no further grants are needed for the per-batch TRUNCATE/INSERT/MERGE cycle.
GRANT USAGE, CREATE ON SCHEMA ingest TO spark_writer;

-- target table in public
GRANT USAGE ON SCHEMA public TO spark_writer;
GRANT INSERT, SELECT ON TABLE public.raw_prices TO spark_writer;

-- DLQ in monitoring
GRANT USAGE ON SCHEMA monitoring TO spark_writer;
GRANT INSERT, SELECT ON TABLE monitoring.dead_letter_events TO spark_writer;

-- =========================================================
-- 2) dbt runner
-- Needs:
--   - read raw_prices (source)
--   - create/replace objects in analytics (models)
--   - delete old records from public + monitoring (automated retention)
-- =========================================================

GRANT USAGE ON SCHEMA public TO dbt_runner;
GRANT SELECT, DELETE ON TABLE public.raw_prices TO dbt_runner;

GRANT USAGE, CREATE ON SCHEMA analytics TO dbt_runner;

-- Retention: dbt_runner deletes rows older than 90 days from the monitoring tables.
-- SELECT is required alongside DELETE because every statement in retention.sql
-- filters on a timestamp column, and PostgreSQL needs read access to any column
-- named in a WHERE clause.
GRANT USAGE ON SCHEMA monitoring TO dbt_runner;
GRANT SELECT, DELETE ON TABLE monitoring.dead_letter_events TO dbt_runner;
GRANT SELECT, DELETE ON TABLE monitoring.alert_events TO dbt_runner;
GRANT SELECT, DELETE ON TABLE monitoring.api_calls TO dbt_runner;
GRANT SELECT, DELETE ON TABLE monitoring.kafka_lag TO dbt_runner;
GRANT SELECT, DELETE ON TABLE monitoring.dbt_test_runs TO dbt_runner;
GRANT SELECT, DELETE ON TABLE monitoring.backup_log TO dbt_runner;

-- run_dbt_test.sh records each dbt test run here, connecting as dbt_runner.
-- This table feeds the "dbt Test Runs" panel and the dbt test failure alert.
GRANT INSERT ON TABLE monitoring.dbt_test_runs TO dbt_runner;
GRANT USAGE, SELECT ON SEQUENCE monitoring.dbt_test_runs_id_seq TO dbt_runner;

-- Auto-grant SELECT to grafana_read on future tables/views created by dbt_runner
-- (so new dbt models are immediately visible in Grafana without manual grants)
ALTER DEFAULT PRIVILEGES FOR USER dbt_runner IN SCHEMA analytics
GRANT SELECT ON TABLES TO grafana_read;

-- =========================================================
-- 3) Grafana read-only
-- Needs:
--   - read raw_prices directly (13 panels chart prices and pipeline health
--     straight from the raw layer instead of going through a mart)
--   - read marts in analytics
--   - read all monitoring tables and views (dashboards + alert rules)
-- =========================================================

GRANT USAGE ON SCHEMA public TO grafana_read;
GRANT SELECT ON TABLE public.raw_prices TO grafana_read;

GRANT USAGE ON SCHEMA analytics TO grafana_read;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO grafana_read;

-- Monitoring tables needed by Grafana dashboard panels and alert rules
-- (Market Overview reads api_calls, kafka_lag; Pipeline & DQ reads dead_letter_events, dbt_test_runs, backup_log)
-- (Alert rules read pipeline_metrics, api_metrics_18m, kafka_lag_latest views + dbt_test_runs, backup_log)
GRANT USAGE ON SCHEMA monitoring TO grafana_read;
GRANT SELECT ON TABLE monitoring.api_calls TO grafana_read;
GRANT SELECT ON TABLE monitoring.dead_letter_events TO grafana_read;
GRANT SELECT ON TABLE monitoring.kafka_lag TO grafana_read;
GRANT SELECT ON TABLE monitoring.alert_events TO grafana_read;
GRANT SELECT ON TABLE monitoring.dbt_test_runs TO grafana_read;
GRANT SELECT ON TABLE monitoring.backup_log TO grafana_read;

-- Monitoring views used by Grafana alert rules and dashboards
GRANT SELECT ON monitoring.pipeline_metrics TO grafana_read;
GRANT SELECT ON monitoring.api_metrics_18m TO grafana_read;
GRANT SELECT ON monitoring.kafka_lag_latest TO grafana_read;

-- =========================================================
-- 4) Producer writer
-- Needs:
--   - insert API call metrics into monitoring.api_calls
-- =========================================================

GRANT USAGE ON SCHEMA monitoring TO producer_writer;
GRANT INSERT ON TABLE monitoring.api_calls TO producer_writer;

-- =========================================================
-- 5) Backup user
-- Needs:
--   - read all schemas for pg_dump
--   - insert into monitoring.backup_log
-- =========================================================

-- pg_dump covers every schema, including ingest, and reads last_value from each
-- sequence, so sequence privileges are as necessary as table privileges.
GRANT USAGE ON SCHEMA public TO backup_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO backup_user;
GRANT USAGE ON SCHEMA analytics TO backup_user;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO backup_user;
GRANT USAGE ON SCHEMA monitoring TO backup_user;
GRANT SELECT ON ALL TABLES IN SCHEMA monitoring TO backup_user;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA monitoring TO backup_user;
GRANT USAGE ON SCHEMA ingest TO backup_user;
GRANT SELECT ON ALL TABLES IN SCHEMA ingest TO backup_user;
GRANT INSERT ON TABLE monitoring.backup_log TO backup_user;

-- The GRANT ... ON ALL statements above are a point-in-time snapshot, and this
-- file runs while analytics and ingest are still empty. Without these default
-- privileges, backups would never cover the dbt models or Spark staging tables.
ALTER DEFAULT PRIVILEGES FOR USER dbt_runner IN SCHEMA analytics
GRANT SELECT ON TABLES TO backup_user;

ALTER DEFAULT PRIVILEGES FOR USER spark_writer IN SCHEMA ingest
GRANT SELECT ON TABLES TO backup_user;

-- =========================================================
-- 6) Alert writer (alert-receiver webhook)
-- Needs:
--   - insert Grafana alert payloads into monitoring.alert_events
-- =========================================================

GRANT USAGE ON SCHEMA monitoring TO alert_writer;
GRANT INSERT ON TABLE monitoring.alert_events TO alert_writer;

-- =========================================================
-- 7) Lag writer (Kafka consumer lag monitor)
-- Needs:
--   - read processed Kafka offsets from public.raw_prices
--   - insert lag snapshots into monitoring.kafka_lag
-- =========================================================

GRANT USAGE ON SCHEMA public TO lag_writer;
GRANT SELECT ON TABLE public.raw_prices TO lag_writer;
GRANT USAGE ON SCHEMA monitoring TO lag_writer;
GRANT INSERT ON TABLE monitoring.kafka_lag TO lag_writer;
