-- =========================================================
-- Database roles for streaming_system.
--
-- Runs automatically as the first init step via
-- docker-entrypoint-initdb.d, or manually against an existing DB:
--   docker exec -i streaming_system-postgres-1 \
--     psql -v ON_ERROR_STOP=1 -U postgres -d "$POSTGRES_DB" -f /ops/sql/roles.sql
--
-- Idempotent: creates each role only if missing, then (re)applies the
-- password from the environment so .env stays the single source of truth.
--
-- Role names are fixed here and in grants.sql. If you change a *_DB_USER
-- value in .env, change it in both files as well.
--
-- No role is a superuser and none may create databases.
-- =========================================================

\getenv spark_pw     SPARK_DB_PASSWORD
\getenv dbt_pw       DBT_DB_PASSWORD
\getenv grafana_pw   GRAFANA_DB_PASSWORD
\getenv producer_pw  PRODUCER_DB_PASSWORD
\getenv backup_pw    BACKUP_DB_PASSWORD
\getenv alert_pw     ALERT_DB_PASSWORD
\getenv lag_pw       LAG_DB_PASSWORD

-- 1) spark_writer — Spark Structured Streaming sink
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'spark_writer') THEN
        CREATE ROLE spark_writer LOGIN;
    END IF;
END $$;
ALTER ROLE spark_writer LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD :'spark_pw';

-- 2) dbt_runner — builds analytics models and runs retention
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dbt_runner') THEN
        CREATE ROLE dbt_runner LOGIN;
    END IF;
END $$;
ALTER ROLE dbt_runner LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD :'dbt_pw';

-- 3) grafana_read — read-only datasource for dashboards and alert rules
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grafana_read') THEN
        CREATE ROLE grafana_read LOGIN;
    END IF;
END $$;
ALTER ROLE grafana_read LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD :'grafana_pw';

-- 4) producer_writer — logs API call metrics
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'producer_writer') THEN
        CREATE ROLE producer_writer LOGIN;
    END IF;
END $$;
ALTER ROLE producer_writer LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD :'producer_pw';

-- 5) backup_user — pg_dump source, writes backup history
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'backup_user') THEN
        CREATE ROLE backup_user LOGIN;
    END IF;
END $$;
ALTER ROLE backup_user LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD :'backup_pw';

-- 6) alert_writer — alert-receiver webhook sink
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'alert_writer') THEN
        CREATE ROLE alert_writer LOGIN;
    END IF;
END $$;
ALTER ROLE alert_writer LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD :'alert_pw';

-- 7) lag_writer — Kafka consumer lag monitor
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'lag_writer') THEN
        CREATE ROLE lag_writer LOGIN;
    END IF;
END $$;
ALTER ROLE lag_writer LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD :'lag_pw';
