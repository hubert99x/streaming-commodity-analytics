-- =========================================================
-- GRANTS for streaming_system
-- Roles assumed to exist:
--   spark_writer, dbt_runner, grafana_read, producer_writer, backup_user
-- Run as postgres superuser.
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

GRANT USAGE, CREATE ON SCHEMA ingest TO spark_writer;

-- DEFAULT PRIVILEGES: auto-grant on future tables created by Spark (one per micro-batch)
ALTER DEFAULT PRIVILEGES IN SCHEMA ingest
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO spark_writer;

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

-- Retention: dbt_runner needs DELETE on monitoring tables for automated cleanup
GRANT USAGE ON SCHEMA monitoring TO dbt_runner;
GRANT DELETE ON TABLE monitoring.dead_letter_events TO dbt_runner;
GRANT DELETE ON TABLE monitoring.alert_events TO dbt_runner;
GRANT DELETE ON TABLE monitoring.api_calls TO dbt_runner;
GRANT DELETE ON TABLE monitoring.kafka_lag TO dbt_runner;
GRANT DELETE ON TABLE monitoring.dbt_test_runs TO dbt_runner;
GRANT DELETE ON TABLE monitoring.backup_log TO dbt_runner;

-- Auto-grant SELECT to grafana_read on future tables/views created by dbt_runner
-- (so new dbt models are immediately visible in Grafana without manual grants)
ALTER DEFAULT PRIVILEGES FOR USER dbt_runner IN SCHEMA analytics
GRANT SELECT ON TABLES TO grafana_read;

-- =========================================================
-- 3) Grafana read-only
-- Needs:
--   - read marts in analytics
--   - read all monitoring tables and views (dashboards + alert rules)
-- =========================================================

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

GRANT USAGE ON SCHEMA public TO backup_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO backup_user;
GRANT USAGE ON SCHEMA analytics TO backup_user;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO backup_user;
GRANT USAGE ON SCHEMA monitoring TO backup_user;
GRANT SELECT ON ALL TABLES IN SCHEMA monitoring TO backup_user;
GRANT INSERT ON TABLE monitoring.backup_log TO backup_user;
