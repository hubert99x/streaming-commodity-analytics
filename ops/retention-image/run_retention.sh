#!/usr/bin/env bash
set -euo pipefail

echo "Starting retention job..."

: "${POSTGRES_HOST:=postgres}"
: "${POSTGRES_PORT:=5432}"
: "${POSTGRES_DB:=commodities}"
: "${POSTGRES_USER:=postgres}"

CONN="host=${POSTGRES_HOST} port=${POSTGRES_PORT} dbname=${POSTGRES_DB} user=${POSTGRES_USER}"

# 1) Delete raw data older than 90 days (balance between analytical depth and storage)
psql "${CONN}" -v ON_ERROR_STOP=1 <<'SQL'
DELETE FROM public.raw_prices
WHERE event_ts < now() - interval '90 days';

ANALYZE public.raw_prices;
SQL

# 2) Cleanup leftover staging tables (Spark batch temp tables)
psql "${CONN}" -v ON_ERROR_STOP=1 <<'SQL'
DO $$
DECLARE r record;
BEGIN
  FOR r IN
    SELECT tablename
    FROM pg_tables
    WHERE schemaname='public'
      AND tablename LIKE 'raw_prices_ingest_%'
  LOOP
    EXECUTE format('DROP TABLE IF EXISTS public.%I', r.tablename);
  END LOOP;
END $$;
SQL

# 3) Weekly VACUUM on Sunday only to avoid blocking Spark writes during peak hours
DOW="$(date +%u)"  # 1..7, 7=Sunday
if [ "${DOW}" = "7" ]; then
  echo "Weekly VACUUM (ANALYZE) running..."
  psql "${CONN}" -v ON_ERROR_STOP=1 -c "VACUUM (ANALYZE) public.raw_prices;"
fi

echo "Retention job finished."
