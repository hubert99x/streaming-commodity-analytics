#!/usr/bin/env bash
# Retention daemon: runs every 24 hours inside a long-lived container.
# Two phases per cycle:
#   1) Delete records older than 90 days from all tables (shared retention.sql)
#   2) Weekly VACUUM ANALYZE on Sundays (reclaims disk space, updates query planner stats)
set -euo pipefail

INTERVAL="${RETENTION_INTERVAL_SEC:-86400}"   # default 24 h

: "${POSTGRES_HOST:=postgres}"
: "${POSTGRES_PORT:=5432}"
: "${POSTGRES_DB:=commodities}"
: "${POSTGRES_USER:=postgres}"

CONN="host=${POSTGRES_HOST} port=${POSTGRES_PORT} dbname=${POSTGRES_DB} user=${POSTGRES_USER}"

echo "Retention daemon started (interval ${INTERVAL}s)."

while true; do
  echo "[$(date -Is)] Running retention cycle..."

  # 1) Run shared retention SQL (delete records older than 90 days)
  if psql "${CONN}" -v ON_ERROR_STOP=1 -f /ops/sql/retention.sql; then
    echo "Retention SQL OK."
  else
    echo "WARNING: retention SQL failed (rc=$?)."
  fi

  # 2) Weekly VACUUM on Sunday only to avoid blocking Spark writes during peak hours
  DOW="$(date +%u)"  # 1..7, 7=Sunday
  if [ "${DOW}" = "7" ]; then
    echo "Weekly VACUUM (ANALYZE) running..."
    psql "${CONN}" -v ON_ERROR_STOP=1 -c "VACUUM (ANALYZE) public.raw_prices;" || \
      echo "WARNING: VACUUM failed."
  fi

  echo "[$(date -Is)] Retention cycle finished. Sleeping ${INTERVAL}s..."
  sleep "${INTERVAL}"
done
