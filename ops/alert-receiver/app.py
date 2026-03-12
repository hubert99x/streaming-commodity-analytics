import json
import os
from datetime import datetime, timezone

import psycopg2
from flask import Flask, request, jsonify

app = Flask(__name__)

# Postgres connection (from env)
PGHOST = os.getenv("POSTGRES_HOST", "postgres")
PGPORT = int(os.getenv("POSTGRES_PORT", "5432"))
PGDATABASE = os.getenv("POSTGRES_DB", "")
PGUSER = os.getenv("POSTGRES_USER", "")
PGPASSWORD = os.getenv("POSTGRES_PASSWORD", "")

# Basic auth token for webhook (optional but recommended)
WEBHOOK_TOKEN = os.getenv("ALERT_WEBHOOK_TOKEN", "")


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


def _get_first_alert(payload: dict) -> dict:
    # Grafana webhook commonly sends {"alerts":[...], ...}
    alerts = payload.get("alerts")
    if isinstance(alerts, list) and alerts:
        a0 = alerts[0]
        if isinstance(a0, dict):
            return a0
    return {}


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
        if token != WEBHOOK_TOKEN:
            return jsonify({"error": "unauthorized"}), 401

    # Parse JSON safely
    try:
        payload = request.get_json(force=True, silent=False)
        if not isinstance(payload, dict):
            raise ValueError("Payload is not a JSON object")
    except Exception as e:
        return jsonify({"error": f"invalid_json: {e}"}), 400

    alert0 = _get_first_alert(payload)

    labels = alert0.get("labels") if isinstance(alert0.get("labels"), dict) else {}
    annotations = alert0.get("annotations") if isinstance(alert0.get("annotations"), dict) else {}

    severity = _pick(labels, "severity", default=None)
    alert_uid = _pick(alert0, "fingerprint", "ruleUid", "uid", default=None)
    alert_title = _pick(annotations, "summary", "title", default=_pick(alert0, "title", default=None))
    state = _pick(alert0, "status", "state", default=_pick(payload, "state", default=None))

    # These fields vary by Grafana version / config, keep best-effort
    org_id = payload.get("orgId")
    dashboard_uid = payload.get("dashboardUID") or payload.get("dashboardUid")
    panel_id = payload.get("panelId")

    # Insert into Postgres (never crash)
    try:
        conn = _connect()
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO monitoring.alert_events
                    (source, severity, alert_uid, alert_title, state, dashboard_uid, panel_id, org_id, raw_payload)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
                    """,
                    (
                        "grafana",
                        severity,
                        alert_uid,
                        alert_title,
                        state,
                        dashboard_uid,
                        panel_id if isinstance(panel_id, int) else None,
                        org_id if isinstance(org_id, int) else None,
                        json.dumps(payload),
                    ),
                )
        conn.close()
    except Exception as e:
        return jsonify({"error": f"db_insert_failed: {e}"}), 500

    return jsonify({"ok": True}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
