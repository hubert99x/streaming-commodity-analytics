#!/usr/bin/env bash
# Retention job: runs as a standalone container (ops profile) on a cron schedule.
# Two phases:
#   1) Delete records older than 90 days from all tables (shared retention.sql)
#   2) Weekly VACUUM ANALYZE on Sundays (reclaims disk space, updates query planner stats)
set -euo pipefail

echo "Starting retention job..."

: "${POSTGRES_HOST:=postgres}"
: "${POSTGRES_PORT:=5432}"
: "${POSTGRES_DB:=commodities}"
: "${POSTGRES_USER:=postgres}"

CONN="host=${POSTGRES_HOST} port=${POSTGRES_PORT} dbname=${POSTGRES_DB} user=${POSTGRES_USER}"

# 1) Run shared retention SQL (delete records older than 90 days)
psql "${CONN}" -v ON_ERROR_STOP=1 -f /ops/sql/retention.sql

# 2) Weekly VACUUM on Sunday only to avoid blocking Spark writes during peak hours
DOW="$(date +%u)"  # 1..7, 7=Sunday
if [ "${DOW}" = "7" ]; then
  echo "Weekly VACUUM (ANALYZE) running..."
  psql "${CONN}" -v ON_ERROR_STOP=1 -c "VACUUM (ANALYZE) public.raw_prices;"
fi

echo "Retention job finished."
