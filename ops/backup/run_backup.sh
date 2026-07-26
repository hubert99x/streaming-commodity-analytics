#!/bin/sh
# Take one pg_dump of the whole database and record the outcome.
#
# Used by two callers so a manual backup behaves exactly like a scheduled one:
#   - the backup-cron container, which loops over this script every 2 hours
#   - `make backup`, which runs it once
#
# Connection settings come from the standard PG* environment variables.
# Exits non-zero when the dump fails, so callers can react.
set -u

KEEP="${BACKUP_KEEP:-360}"          # 360 dumps = 30 days at the 2h cadence
BACKUP_DIR="${BACKUP_DIR:-/backups}"

ts="$(date +%Y%m%d_%H%M%S)"         # seconds included to avoid collisions on restart
out="${BACKUP_DIR}/backup_${ts}.dump"
base="$(basename "$out")"
err_file="/tmp/pg_dump_err_${ts}.log"

log_backup() {
    # Best-effort: a logging failure must never fail the backup itself
    psql -X -v ON_ERROR_STOP=1 -c "$1" >/dev/null 2>&1 \
        || echo "WARNING: failed to write monitoring.backup_log"
}

echo "Running backup at $(date -Is) -> $out"

# --lock-wait-timeout makes the dump fail fast instead of hanging behind a lock
if pg_dump --lock-wait-timeout=30s -F c -f "$out" 2> "$err_file"; then
    size="$(stat -c%s "$out" 2>/dev/null || echo 0)"
    echo "Backup OK: $out ($size bytes)"

    log_backup "INSERT INTO monitoring.backup_log (file_name, file_size_bytes, status, error_msg)
                VALUES ('${base}', ${size}, 'OK', NULL);"

    rm -f "$err_file" >/dev/null 2>&1 || true

    # Prune oldest dumps beyond the retention count
    ls -1t "${BACKUP_DIR}"/backup_*.dump 2>/dev/null | tail -n "+$((KEEP + 1))" | xargs -r rm -f
    exit 0
fi

# Keep more than the last line so the logged reason is actually diagnosable
err_msg="$(tail -n 20 "$err_file" 2>/dev/null | tr '\n' ' ' | tr -d '\r' | head -c 1500)"
err_sql="$(printf "%s" "$err_msg" | sed "s/'/''/g")"

echo "Backup FAILED at $(date -Is): $err_msg"
rm -f "$out" >/dev/null 2>&1 || true

log_backup "INSERT INTO monitoring.backup_log (file_name, file_size_bytes, status, error_msg)
            VALUES ('${base}', NULL, 'FAILED', '${err_sql}');"

rm -f "$err_file" >/dev/null 2>&1 || true
exit 1
