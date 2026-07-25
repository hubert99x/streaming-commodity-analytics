-- =========================================================
-- ONE-OFF cleanup of objects left behind by earlier versions of the system.
--
-- NOT part of the automatic bootstrap: this file is deliberately absent from
-- docker-entrypoint-initdb.d. Run it by hand, once, against a database that
-- has been carrying state since before those versions were replaced:
--
--   docker exec -i streaming_system-postgres-1 \
--     psql -v ON_ERROR_STOP=1 -U postgres -d "$POSTGRES_DB" \
--     -f /ops/sql/cleanup_legacy.sql
--
-- Take a backup first — every statement here is a DROP.
-- A database created from ops/sql/init.sql already lacks all of these
-- objects, so running this on a fresh install is a no-op.
--
-- Verified before writing this file (2026-07-26): the staging tables held
-- 0 rows, the dead columns were 100% NULL, and the legacy roles owned no
-- objects and could not log in.
-- =========================================================

BEGIN;

-- ---------------------------------------------------------
-- 1) Per-batch staging tables from the old Spark sink
--
-- An earlier implementation created one staging table per micro-batch
-- (raw_prices_ingest_stream1_<batchId>). The current job uses two persistent
-- tables — ingest.raw_prices_staging and ingest.dlq_staging — truncated per
-- batch. The old tables are empty but still land in every pg_dump.
-- ---------------------------------------------------------
DO $$
DECLARE
    r        record;
    dropped  integer := 0;
BEGIN
    FOR r IN
        SELECT n.nspname AS schema_name, c.relname AS table_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'ingest'
          AND c.relkind = 'r'
          AND c.relname ~ '^raw_prices_ingest_stream1_[0-9]+$'
    LOOP
        EXECUTE format('DROP TABLE IF EXISTS %I.%I', r.schema_name, r.table_name);
        dropped := dropped + 1;
    END LOOP;
    RAISE NOTICE 'dropped % per-batch staging table(s)', dropped;
END $$;

-- ---------------------------------------------------------
-- 2) Renamed dbt model left behind as a plain table
--
-- analytics.mart_price_stats has the same columns as mart_minute_last_price
-- and stopped receiving data when the model was renamed. No dbt model, Grafana
-- panel or alert rule references it.
--
-- It does still hold historical rows. Comment this statement out if you want
-- to keep them; otherwise export first:
--   \copy analytics.mart_price_stats TO 'mart_price_stats.csv' CSV HEADER
-- ---------------------------------------------------------
DROP TABLE IF EXISTS analytics.mart_price_stats;

-- ---------------------------------------------------------
-- 3) Duplicate index on mart_minute_last_price
--
-- dbt merges post-hooks from dbt_project.yml and from the model file, so two
-- indexes were created on the same table. The project-level hook is gone; this
-- drops the index it left behind. dbt recreates the remaining one on each run.
-- ---------------------------------------------------------
DROP INDEX IF EXISTS analytics.idx_mart_minute_last_price_symbol_bucket;

-- ---------------------------------------------------------
-- 4) Columns nothing writes to
--
-- run_dbt_test.sh inserts only environment/status/total/pass/warn/error/fail/
-- skipped. These two columns are absent from ops/sql/init.sql, so keeping them
-- makes an existing database diverge from a freshly created one.
-- ---------------------------------------------------------
ALTER TABLE monitoring.dbt_test_runs DROP COLUMN IF EXISTS elapsed_seconds;
ALTER TABLE monitoring.dbt_test_runs DROP COLUMN IF EXISTS invocation_id;

-- ---------------------------------------------------------
-- 5) Roles from an earlier naming convention
--
-- role_* duplicates sit next to the seven roles the system actually uses.
-- They own no objects and cannot log in, but they still hold grants, so
-- DROP OWNED BY must revoke those before DROP ROLE succeeds.
-- ---------------------------------------------------------
DO $$
DECLARE
    legacy_role text;
BEGIN
    FOREACH legacy_role IN ARRAY ARRAY[
        'role_spark_writer',
        'role_dbt_runner',
        'role_grafana_read',
        'role_producer_writer',
        'role_backup_read'
    ] LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = legacy_role) THEN
            EXECUTE format('DROP OWNED BY %I', legacy_role);
            EXECUTE format('DROP ROLE %I', legacy_role);
            RAISE NOTICE 'dropped legacy role %', legacy_role;
        END IF;
    END LOOP;
END $$;

COMMIT;

-- VACUUM cannot run inside a transaction block; reclaim the freed space with
-- ops/sql/vacuum.sql afterwards if the database is large.
