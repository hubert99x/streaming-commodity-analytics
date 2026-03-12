-- =========================================================
-- GRANTS for streaming_system
-- Roles assumed to exist:
--   spark_writer, dbt_runner, grafana_read
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
-- =========================================================

GRANT USAGE ON SCHEMA public TO dbt_runner;
GRANT SELECT ON TABLE public.raw_prices TO dbt_runner;

GRANT USAGE, CREATE ON SCHEMA analytics TO dbt_runner;

-- Auto-grant SELECT to grafana_read on future tables/views created by dbt_runner
-- (so new dbt models are immediately visible in Grafana without manual grants)
ALTER DEFAULT PRIVILEGES FOR USER dbt_runner IN SCHEMA analytics
GRANT SELECT ON TABLES TO grafana_read;

-- =========================================================
-- 3) Grafana read-only
-- Needs:
--   - read marts in analytics
--   - (optional) read dq table monitoring.dbt_test_runs
-- =========================================================

GRANT USAGE ON SCHEMA analytics TO grafana_read;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO grafana_read;

-- (optional but usually needed for panels)
GRANT USAGE ON SCHEMA monitoring TO grafana_read;
GRANT SELECT ON TABLE monitoring.dbt_test_runs TO grafana_read;
