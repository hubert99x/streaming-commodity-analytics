#!/usr/bin/env bash
# Run dbt tests independently and log results to monitoring.dbt_test_runs.
# Parses run_results.json to extract pass/warn/error/fail/skip counts,
# determines aggregate status, and inserts a summary row into Postgres.
# This enables the Grafana "dbt Test Runs" panel to show test history.
set -euo pipefail

DQ_ENVIRONMENT="${DQ_ENVIRONMENT:-dev}"
DBT_TARGET="${DBT_TARGET:-dev}"
DBT_TARGET_PATH="${DBT_TARGET_PATH:-target}"
RR="${DBT_TARGET_PATH%/}/run_results.json"

cd /dbt

echo "[dbt-test] $(date -Is) starting..."
echo "[dbt-test] target=${DBT_TARGET}"
echo "[dbt-test] target_path=${DBT_TARGET_PATH}"

# || true: always parse results even if some tests fail (we log the outcome below)
dbt test --target "$DBT_TARGET" --no-use-colors || true

echo "[dbt-test] parsing results from $RR ..."

if [ ! -f "$RR" ]; then
  echo "[dbt-test] run_results.json not found at $RR"
  exit 1
fi

# dbt statuses: pass/warn/error/fail/skipped
# "fail" = test assertion failed, "error" = execution error (e.g. SQL syntax)
pass=$(jq '[.results[] | select(.status=="pass")] | length' "$RR")
warn=$(jq '[.results[] | select(.status=="warn")] | length' "$RR")
error=$(jq '[.results[] | select(.status=="error")] | length' "$RR")
skip=$(jq '[.results[] | select(.status=="skipped")] | length' "$RR")
fail=$(jq '[.results[] | select(.status=="fail")] | length' "$RR" 2>/dev/null || echo 0)
total=$(jq '.results | length' "$RR")

# Aggregate status: any error/fail -> FAIL, else any warn -> WARN, else PASS
status="PASS"
if [ "$error" -gt 0 ] || [ "$fail" -gt 0 ]; then
  status="FAIL"
elif [ "$warn" -gt 0 ]; then
  status="WARN"
fi

echo "[dbt-test] status=$status total=$total pass=$pass warn=$warn error=$error fail=$fail skip=$skip"

# Use parameterized query via psql variables to prevent SQL injection
# Note: psql ignores -v variables in -c mode, so we pipe the query via stdin
PGPASSWORD="${POSTGRES_PASSWORD}" psql -h "${POSTGRES_HOST}" -p "${POSTGRES_PORT}" -d "${POSTGRES_DB}" -U "${POSTGRES_USER}" \
  -v ON_ERROR_STOP=1 \
  -v v_env="$DQ_ENVIRONMENT" \
  -v v_status="$status" \
  -v v_total="$total" \
  -v v_pass="$pass" \
  -v v_warn="$warn" \
  -v v_error="$error" \
  -v v_fail="$fail" \
  -v v_skip="$skip" \
  <<'EOSQL'
INSERT INTO monitoring.dbt_test_runs
    (environment, status, total, pass, warn, error, fail, skipped)
    VALUES
    (:'v_env', :'v_status', :v_total, :v_pass, :v_warn, :v_error, :v_fail, :v_skip);
EOSQL

echo "[dbt-test] $(date -Is) done."
