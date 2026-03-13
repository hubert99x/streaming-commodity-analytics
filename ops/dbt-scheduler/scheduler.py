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
DBT_TARGET = os.getenv("DBT_TARGET", "dev")

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

    try:
        print(f"[dbt-scheduler] {_now_iso()} starting dbt build...", flush=True)
        result = subprocess.run(
            ["dbt", "build", "--target", DBT_TARGET, "--no-use-colors"],
            cwd="/dbt",
            capture_output=False,
            timeout=300,
        )
        _last_build_ok = result.returncode == 0
        if _last_build_ok:
            print(f"[dbt-scheduler] {_now_iso()} dbt build OK", flush=True)
        else:
            print(
                f"[dbt-scheduler] {_now_iso()} dbt build FAILED (exit={result.returncode})",
                flush=True,
            )
        return _last_build_ok
    except subprocess.TimeoutExpired:
        print(f"[dbt-scheduler] {_now_iso()} dbt build TIMEOUT (300s)", flush=True)
        _last_build_ok = False
        return False
    except Exception as e:
        print(f"[dbt-scheduler] {_now_iso()} dbt build ERROR: {e}", flush=True)
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


def _handle_stop(signum, _frame):
    global _running
    _running = False
    print(f"[dbt-scheduler] {_now_iso()} received signal {signum}, stopping...", flush=True)


def main():
    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    print(
        f"[dbt-scheduler] {_now_iso()} scheduler started "
        f"(build every {DBT_RUN_INTERVAL}s, test every {DBT_TEST_INTERVAL}s)",
        flush=True,
    )

    last_build = 0.0
    last_test = 0.0

    while _running:
        _touch_health()
        now = time.monotonic()

        if now - last_build >= DBT_RUN_INTERVAL:
            _run_dbt_build()
            last_build = time.monotonic()

        if now - last_test >= DBT_TEST_INTERVAL:
            _run_dbt_test_and_log()
            last_test = time.monotonic()

        # Sleep in 1-second ticks for responsive shutdown
        slept = 0
        while _running and slept < 30:
            time.sleep(1)
            slept += 1

    print(f"[dbt-scheduler] {_now_iso()} scheduler stopped.", flush=True)


if __name__ == "__main__":
    main()
