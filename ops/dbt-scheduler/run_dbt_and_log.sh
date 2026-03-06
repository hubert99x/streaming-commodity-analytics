#!/usr/bin/env sh
set -eu

DQ_ENVIRONMENT="${DQ_ENVIRONMENT:-dev}"
DBT_TARGET="${DBT_TARGET:-dev}"

cd /dbt

t0=$(date +%s)

dbt deps
dbt build --target "$DBT_TARGET"
dbt test --target "$DBT_TARGET" || true

t1=$(date +%s)
elapsed=$((t1 - t0))

RR="target/run_results.json"
if [ ! -f "$RR" ]; then
  echo "run_results.json not found at $RR"
  exit 1
fi

pass=$(jq '[.results[] | select(.status=="pass")] | length' "$RR")
warn=$(jq '[.results[] | select(.status=="warn")] | length' "$RR")
error=$(jq '[.results[] | select(.status=="error")] | length' "$RR")
skip=$(jq '[.results[] | select(.status=="skipped")] | length' "$RR")
total=$(jq '.results | length' "$RR")
fail=$(jq '[.results[] | select(.status=="fail")] | length' "$RR" 2>/dev/null || echo 0)
invocation_id=$(jq -r '.metadata.invocation_id // empty' "$RR")

status="PASS"
if [ "$error" -gt 0 ] || [ "$fail" -gt 0 ]; then
  status="FAIL"
elif [ "$warn" -gt 0 ]; then
  status="WARN"
fi

psql "host=${POSTGRES_HOST} port=${POSTGRES_PORT} dbname=${POSTGRES_DB} user=${POSTGRES_USER} password=${POSTGRES_PASSWORD}" \
  -v ON_ERROR_STOP=1 \
  -c "INSERT INTO monitoring.dbt_test_runs
      (environment, status, total, pass, warn, error, fail, skipped, elapsed_seconds, invocation_id)
      VALUES
      ('${DQ_ENVIRONMENT}', '${status}', ${total}, ${pass}, ${warn}, ${error}, ${fail}, ${skip}, ${elapsed}, '${invocation_id}');"