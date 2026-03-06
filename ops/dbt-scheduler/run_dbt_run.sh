#!/usr/bin/env bash
set -euo pipefail

# dbt target must exist in profiles.yml (you currently have only: dev)
DBT_TARGET="${DBT_TARGET:-dev}"

cd /dbt

echo "[dbt-run] $(date -Is) starting..."
dbt run --target "$DBT_TARGET" --no-use-colors
echo "[dbt-run] $(date -Is) done."