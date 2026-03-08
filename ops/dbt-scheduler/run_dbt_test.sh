#!/usr/bin/env bash
set -euo pipefail

# Default environment label (for Postgres logging)
DQ_ENVIRONMENT="${DQ_ENVIRONMENT:-dev}"

# dbt target must exist in profiles.yml
DBT_TARGET="${DBT_TARGET:-dev}"

# Respect dbt target path from environment
DBT_TARGET_PATH="${DBT_TARGET_PATH:-target}"
RR="${DBT_TARGET_PATH%/}/run_results.json"

cd /dbt

echo "[dbt-test] $(date -Is) starting..."
echo "[dbt-test] target=${DBT_TARGET}"
echo "[dbt-test] target_path=${DBT_TARGET_PATH}"

# Run dbt tests (never crash the container on test failures)
dbt test --target "$DBT_TARGET" --no-use-colors || true

echo "[dbt-test] parsing results from $RR ..."

if [ ! -f "$RR" ]; then
  echo "[dbt-test] run_results.json not found at $RR"
  exit 1
fi

pass=$(jq '[.results[] | select(.status=="pass")] | length' "$RR")
warn=$(jq '[.results[] | select(.status=="warn")] | length' "$RR")
error=$(jq '[.results[] | select(.status=="error")] | length' "$RR")
skip=$(jq '[.results[] | select(.status=="skipped")] | length' "$RR")
fail=$(jq '[.results[] | select(.status=="fail")] | length' "$RR" 2>/dev/null || echo 0)
total=$(jq '.results | length' "$RR")

status="PASS"
if [ "$error" -gt 0 ] || [ "$fail" -gt 0 ]; then
  status="FAIL"
elif [ "$warn" -gt 0 ]; then
  status="WARN"
fi

echo "[dbt-test] status=$status total=$total pass=$pass warn=$warn error=$error fail=$fail skip=$skip"

psql "host=${POSTGRES_HOST} port=${POSTGRES_PORT} dbname=${POSTGRES_DB} user=${POSTGRES_USER} password=${POSTGRES_PASSWORD}" \
  -v ON_ERROR_STOP=1 \
  -c "INSERT INTO monitoring.dbt_test_runs
      (environment, status, total, pass, warn, error, fail, skipped)
      VALUES
      ('${DQ_ENVIRONMENT}', '${status}', ${total}, ${pass}, ${warn}, ${error}, ${fail}, ${skip});"

echo "[dbt-test] $(date -Is) done."