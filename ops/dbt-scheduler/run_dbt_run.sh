#!/usr/bin/env bash
set -euo pipefail

DBT_TARGET="${DBT_TARGET:-dev}"

cd /dbt

echo "[dbt-run] $(date -Is) starting dbt build (models + tests)..."
dbt build --target "$DBT_TARGET" --no-use-colors
rc=$?
echo "[dbt-run] $(date -Is) done (exit=$rc)."
exit $rc