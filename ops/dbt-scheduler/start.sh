#!/usr/bin/env bash
set -euo pipefail

echo "[startup] $(date -Is) Running dbt deps once..."

cd /dbt
dbt deps --no-use-colors

echo "[startup] $(date -Is) Starting scheduler..."
exec python3 /ops/scheduler.py
