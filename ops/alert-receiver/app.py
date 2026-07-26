"""
Grafana alert webhook receiver.

Listens for Grafana alert notifications on POST /grafana,
extracts alert metadata, and logs them to monitoring.alert_events.
"""

import contextlib
import hmac
import json
import os
import sys
from datetime import datetime, timezone

import psycopg2
from flask import Flask, request, jsonify

app = Flask(__name__)
PGHOST = os.getenv("POSTGRES_HOST", "postgres")
PGPORT = int(os.getenv("POSTGRES_PORT", "5432"))
PGDATABASE = os.getenv("POSTGRES_DB", "")
PGUSER = os.getenv("POSTGRES_USER", "")
PGPASSWORD = os.getenv("POSTGRES_PASSWORD", "")

# Basic auth token for webhook (optional but recommended)
WEBHOOK_TOKEN = os.getenv("ALERT_WEBHOOK_TOKEN", "")
WEBHOOK_AUTH_DISABLED = os.getenv("ALERT_WEBHOOK_AUTH_DISABLED", "").lower() == "true"

if not WEBHOOK_TOKEN and not WEBHOOK_AUTH_DISABLED:
    print(
        "FATAL: ALERT_WEBHOOK_TOKEN is not set. "
        "Set it in .env or set ALERT_WEBHOOK_AUTH_DISABLED=true to run without auth.",
        file=sys.stderr,
        flush=True,
    )
    sys.exit(1)


def _utc_now():
    return datetime.now(timezone.utc)


def _connect():
    return psycopg2.connect(
        host=PGHOST,
        port=PGPORT,
        dbname=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD,
    )


def _get_alerts(payload: dict) -> list:
    """
    Extract every alert from Grafana's {"alerts":[...]} webhook payload.

    Notification policies group alerts by alertname, so one POST can carry
    several firing alerts — each one gets its own monitoring.alert_events row.
    Payloads without a usable alerts array still produce a single row so the
    raw payload is never lost.
    """
    alerts = payload.get("alerts")
    if isinstance(alerts, list):
        found = [a for a in alerts if isinstance(a, dict)]
        if found:
            return found
    return [{}]


def _pick(d: dict, *keys, default=None):
    """Return first non-None value from candidate keys (handles Grafana version differences)."""
    for k in keys:
        v = d.get(k)
        if v is not None:
            return v
    return default


@app.get("/health")
def health():
    return jsonify({"status": "ok", "ts_utc": _utc_now().isoformat()}), 200


@app.post("/grafana")
def grafana_webhook():
    # Optional token check
    if WEBHOOK_TOKEN:
        token = request.headers.get("X-Webhook-Token", "")
        if not hmac.compare_digest(token, WEBHOOK_TOKEN):
            return jsonify({"error": "unauthorized"}), 401

    # Parse JSON safely
    try:
        payload = request.get_json(force=True, silent=False)
        if not isinstance(payload, dict):
            raise ValueError("Payload is not a JSON object")
    except Exception as e:
        return jsonify({"error": f"invalid_json: {e}"}), 400

    # These fields vary by Grafana version / config, extract best-effort
    org_id = payload.get("orgId")
    dashboard_uid = payload.get("dashboardUID") or payload.get("dashboardUid")
    panel_id = payload.get("panelId")
    raw_payload = json.dumps(payload)

    rows = []
    for alert in _get_alerts(payload):
        labels = alert.get("labels") if isinstance(alert.get("labels"), dict) else {}
        annotations = alert.get("annotations") if isinstance(alert.get("annotations"), dict) else {}

        # Fallback field names handle different Grafana versions
        # (v9 uses "fingerprint", v10 uses "ruleUid", etc.)
        rows.append((
            "grafana",
            _pick(labels, "severity", default=None),
            _pick(alert, "fingerprint", "ruleUid", "uid", default=None),
            _pick(annotations, "summary", "title", default=_pick(alert, "title", default=None)),
            _pick(alert, "status", "state", default=_pick(payload, "state", default=None)),
            dashboard_uid,
            panel_id if isinstance(panel_id, int) else None,
            org_id if isinstance(org_id, int) else None,
            raw_payload,
        ))

    # A DB failure returns 500 rather than raising, so Grafana retries the
    # delivery instead of dropping the alert.
    try:
        with contextlib.closing(_connect()) as conn:
            with conn:
                with conn.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO monitoring.alert_events
                        (source, severity, alert_uid, alert_title, state, dashboard_uid, panel_id, org_id, raw_payload)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
                        """,
                        rows,
                    )
    except Exception as e:
        return jsonify({"error": f"db_insert_failed: {e}"}), 500

    return jsonify({"ok": True, "stored": len(rows)}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
