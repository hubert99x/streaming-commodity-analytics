#!/usr/bin/env bash
set -euo pipefail

mkdir -p /var/log
touch /var/log/dbt_run.log
touch /var/log/dbt_test.log

# Export only safe env vars for cron jobs (cron runs without the container's env)
python3 - <<'PY'
import os, shlex

allow_prefixes = ("POSTGRES_", "DBT_")
allow_exact = {"TZ", "DQ_ENVIRONMENT"}

with open("/ops/container_env.sh", "w", encoding="utf-8") as f:
    f.write("#!/usr/bin/env bash\n")
    f.write("set -euo pipefail\n")
    for k, v in sorted(os.environ.items()):
        if k.startswith(allow_prefixes) or k in allow_exact:
            f.write(f"export {k}={shlex.quote(v)}\n")
PY

chmod 700 /ops/container_env.sh

echo "[startup] $(date -Is) Running dbt deps once..."

cd /dbt
dbt deps --no-use-colors

cat > /etc/cron.d/dbt <<'EOF'
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# dbt run every 6 minutes (matches producer INTERVAL_SEC)
*/6 * * * * root bash -lc "source /ops/container_env.sh && /ops/run_dbt_run.sh" >> /var/log/dbt_run.log 2>&1

# dbt test every 30 minutes (less frequent - tests are heavier than model refreshes)
*/30 * * * * root bash -lc "source /ops/container_env.sh && /ops/run_dbt_test.sh" >> /var/log/dbt_test.log 2>&1

# cleanup ingest staging tables every hour (keep last 2000)
0 * * * * root bash -lc 'source /ops/container_env.sh && psql "host=${POSTGRES_HOST} port=${POSTGRES_PORT} dbname=${POSTGRES_DB} user=${POSTGRES_USER} password=${POSTGRES_PASSWORD}" -f /ops/sql/cleanup_ingest_keep_2000.sql && echo "[cleanup] $(date -Is) done"' >> /var/log/dbt_run.log 2>&1

EOF

chmod 0644 /etc/cron.d/dbt

echo "[startup] $(date -Is) Starting cron..."
cron

echo "[startup] $(date -Is) Tailing logs..."
tail -F /var/log/dbt_run.log /var/log/dbt_test.log