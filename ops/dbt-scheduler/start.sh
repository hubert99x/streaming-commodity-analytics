#!/usr/bin/env bash
# Entrypoint for the dbt-scheduler container.
# 1) Installs dbt package dependencies once on startup
# 2) Hands off to the Python scheduler daemon (which runs dbt build/test on intervals)
set -euo pipefail

echo "[startup] $(date -Is) Running dbt deps once..."

cd /dbt
dbt deps --no-use-colors

echo "[startup] $(date -Is) Starting scheduler..."
# exec replaces this shell process with Python (PID 1 for proper signal handling)
exec python3 /ops/scheduler.py
