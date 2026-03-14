#!/usr/bin/env bash
# Run dbt build (compiles models + executes tests in dependency order).
# Called by the scheduler or manually via: docker compose exec dbt-scheduler bash /ops/run_dbt_run.sh
set -euo pipefail

DBT_TARGET="${DBT_TARGET:-dev}"

cd /dbt

echo "[dbt-run] $(date -Is) starting dbt build (models + tests)..."
dbt build --target "$DBT_TARGET" --no-use-colors
rc=$?
echo "[dbt-run] $(date -Is) done (exit=$rc)."
exit $rc