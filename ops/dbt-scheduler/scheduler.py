"""
dbt scheduler — replaces cron with a Python loop.

Advantages over cron:
- Inherits container env vars directly (no env export hacks)
- Prevents overlapping runs via threading lock
- Logs dbt build failures with exit codes (no silent || true)
- Supports container healthcheck via /tmp/dbt_scheduler_alive
"""

import os
import signal
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

DBT_RUN_INTERVAL = int(os.getenv("DBT_RUN_INTERVAL_SEC", "360"))  # 6 minutes
DBT_TEST_INTERVAL = int(os.getenv("DBT_TEST_INTERVAL_SEC", "1800"))  # 30 minutes
INGEST_CLEANUP_INTERVAL = int(os.getenv("INGEST_CLEANUP_INTERVAL_SEC", "3600"))  # 1 hour
DBT_TARGET = os.getenv("DBT_TARGET", "dev")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "commodities")
POSTGRES_USER = os.getenv("POSTGRES_USER", "")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")

HEALTH_FILE = Path("/tmp/dbt_scheduler_alive")

_lock = threading.Lock()
_running = True
_last_build_ok = True


def _now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S%z")


def _touch_health():
    """Write health marker so container healthcheck can verify scheduler is alive."""
    try:
        HEALTH_FILE.write_text(_now_iso())
    except OSError:
        pass


def _run_dbt_build():
    """Run dbt build (models + tests). Returns True on success."""
    global _last_build_ok
    if not _lock.acquire(blocking=False):
        print(f"[dbt-scheduler] {_now_iso()} SKIP dbt build — previous run still active", flush=True)
        return _last_build_ok

    t0 = time.monotonic()
    try:
        print(f"[dbt-scheduler] {_now_iso()} starting dbt build...", flush=True)
        result = subprocess.run(
            ["dbt", "build", "--target", DBT_TARGET, "--no-use-colors"],
            cwd="/dbt",
            capture_output=False,
            timeout=300,
        )
        duration_ms = int((time.monotonic() - t0) * 1000)
        _last_build_ok = result.returncode == 0
        if _last_build_ok:
            print(f"[dbt-scheduler] {_now_iso()} dbt build OK duration_ms={duration_ms}", flush=True)
        else:
            print(
                f"[dbt-scheduler] {_now_iso()} dbt build FAILED (exit={result.returncode}) duration_ms={duration_ms}",
                flush=True,
            )
        return _last_build_ok
    except subprocess.TimeoutExpired:
        duration_ms = int((time.monotonic() - t0) * 1000)
        print(f"[dbt-scheduler] {_now_iso()} dbt build TIMEOUT (300s) duration_ms={duration_ms}", flush=True)
        _last_build_ok = False
        return False
    except Exception as e:
        duration_ms = int((time.monotonic() - t0) * 1000)
        print(f"[dbt-scheduler] {_now_iso()} dbt build ERROR: {e} duration_ms={duration_ms}", flush=True)
        _last_build_ok = False
        return False
    finally:
        _lock.release()


def _run_dbt_test_and_log():
    """Run dbt test and log results to monitoring.dbt_test_runs."""
    if not _lock.acquire(blocking=False):
        print(f"[dbt-scheduler] {_now_iso()} SKIP dbt test — build still active", flush=True)
        return

    try:
        print(f"[dbt-scheduler] {_now_iso()} starting dbt test + log...", flush=True)
        result = subprocess.run(
            ["bash", "/ops/run_dbt_test.sh"],
            cwd="/dbt",
            capture_output=False,
            timeout=300,
        )
        if result.returncode != 0:
            print(
                f"[dbt-scheduler] {_now_iso()} dbt test log FAILED (exit={result.returncode})",
                flush=True,
            )
        else:
            print(f"[dbt-scheduler] {_now_iso()} dbt test log OK", flush=True)
    except subprocess.TimeoutExpired:
        print(f"[dbt-scheduler] {_now_iso()} dbt test TIMEOUT (300s)", flush=True)
    except Exception as e:
        print(f"[dbt-scheduler] {_now_iso()} dbt test ERROR: {e}", flush=True)
    finally:
        _lock.release()


def _cleanup_ingest_tables():
    """Drop old Spark staging tables, keeping 2000 most recent (automated version of cleanup_ingest_keep_2000.sql)."""
    try:
        print(f"[dbt-scheduler] {_now_iso()} starting ingest cleanup...", flush=True)
        result = subprocess.run(
            [
                "psql",
                "-X",
                "-h", POSTGRES_HOST,
                "-p", POSTGRES_PORT,
                "-U", POSTGRES_USER,
                "-d", POSTGRES_DB,
                "-v", "ON_ERROR_STOP=1",
                "-f", "/ops/sql/cleanup_ingest_keep_2000.sql",
            ],
            capture_output=True,
            text=True,
            timeout=60,
            env={**os.environ, "PGPASSWORD": POSTGRES_PASSWORD},
        )
        if result.returncode == 0:
            print(f"[dbt-scheduler] {_now_iso()} ingest cleanup OK", flush=True)
        else:
            print(
                f"[dbt-scheduler] {_now_iso()} ingest cleanup FAILED (exit={result.returncode}): {result.stderr[:200]}",
                flush=True,
            )
    except subprocess.TimeoutExpired:
        print(f"[dbt-scheduler] {_now_iso()} ingest cleanup TIMEOUT", flush=True)
    except Exception as e:
        print(f"[dbt-scheduler] {_now_iso()} ingest cleanup ERROR: {e}", flush=True)


def _handle_stop(signum, _frame):
    global _running
    _running = False
    print(f"[dbt-scheduler] {_now_iso()} received signal {signum}, stopping...", flush=True)


def main():
    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    print(
        f"[dbt-scheduler] {_now_iso()} scheduler started "
        f"(build every {DBT_RUN_INTERVAL}s, test every {DBT_TEST_INTERVAL}s, "
        f"ingest cleanup every {INGEST_CLEANUP_INTERVAL}s)",
        flush=True,
    )

    last_build = 0.0
    last_test = 0.0
    last_cleanup = 0.0

    while _running:
        _touch_health()
        now = time.monotonic()

        if now - last_build >= DBT_RUN_INTERVAL:
            _run_dbt_build()
            last_build = time.monotonic()

        if now - last_test >= DBT_TEST_INTERVAL:
            _run_dbt_test_and_log()
            last_test = time.monotonic()

        if now - last_cleanup >= INGEST_CLEANUP_INTERVAL:
            _cleanup_ingest_tables()
            last_cleanup = time.monotonic()

        # Sleep in 1-second ticks for responsive shutdown
        slept = 0
        while _running and slept < 30:
            time.sleep(1)
            slept += 1

    print(f"[dbt-scheduler] {_now_iso()} scheduler stopped.", flush=True)


if __name__ == "__main__":
    main()
