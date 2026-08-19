"""Tests for the Grafana alert webhook receiver."""

import importlib.util
import os
import pathlib

import pytest

# The receiver needs Flask and waitress, which are not part of the producer
# requirements installed for the other test modules.
pytest.importorskip(
    "flask",
    reason="install with: pip install -r ops/alert-receiver/requirements.txt",
)
pytest.importorskip(
    "waitress",
    reason="install with: pip install -r ops/alert-receiver/requirements.txt",
)

# app.py exits at import time unless webhook auth is configured, so the token has
# to exist before the module is loaded. alert-receiver also lives in a directory
# whose name is not a valid module name, hence the explicit spec loading.
TOKEN = "test-token"
os.environ["ALERT_WEBHOOK_TOKEN"] = TOKEN

_spec = importlib.util.spec_from_file_location(
    "alert_receiver_app",
    pathlib.Path(__file__).resolve().parents[1] / "ops" / "alert-receiver" / "app.py",
)
receiver = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(receiver)


# ---- Fake psycopg2 objects -------------------------------------------------
# The insert runs as `with closing(conn) as conn, conn, conn.cursor() as cur`,
# so the connection is used both as a closeable and as a transaction context.

class _FakeCursor:
    def __init__(self, sink):
        self._sink = sink

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def executemany(self, sql, rows):
        self._sink.extend(rows)


class _FakeConn:
    def __init__(self, sink):
        self._sink = sink
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def cursor(self):
        return _FakeCursor(self._sink)

    def close(self):
        self.closed = True


@pytest.fixture
def stored_rows(monkeypatch):
    """Capture the rows the receiver would insert, without touching Postgres."""
    rows = []
    monkeypatch.setattr(receiver, "_connect", lambda: _FakeConn(rows))
    return rows


@pytest.fixture
def client():
    return receiver.app.test_client()


def _post(client, payload, token=TOKEN):
    return client.post("/grafana", json=payload, headers={"X-Webhook-Token": token})


# ---- _get_alerts -----------------------------------------------------------

class TestGetAlerts:
    def test_returns_every_alert_from_the_payload(self):
        payload = {"alerts": [{"status": "firing"}, {"status": "resolved"}]}
        assert receiver._get_alerts(payload) == [{"status": "firing"}, {"status": "resolved"}]

    def test_payload_without_alerts_key_still_yields_one_row(self):
        assert receiver._get_alerts({"state": "alerting"}) == [{}]

    def test_alerts_that_is_not_a_list_yields_one_row(self):
        assert receiver._get_alerts({"alerts": "firing"}) == [{}]

    def test_empty_alerts_list_yields_one_row(self):
        assert receiver._get_alerts({"alerts": []}) == [{}]

    def test_non_dict_entries_are_dropped(self):
        payload = {"alerts": ["broken", {"status": "firing"}, None]}
        assert receiver._get_alerts(payload) == [{"status": "firing"}]

    def test_list_of_only_non_dict_entries_yields_one_row(self):
        assert receiver._get_alerts({"alerts": ["broken", None]}) == [{}]


# ---- _pick -----------------------------------------------------------------

class TestPick:
    def test_returns_the_first_key_that_is_present(self):
        assert receiver._pick({"fingerprint": "abc", "ruleUid": "def"}, "fingerprint", "ruleUid") == "abc"

    def test_falls_through_to_a_later_key(self):
        assert receiver._pick({"ruleUid": "def"}, "fingerprint", "ruleUid") == "def"

    def test_returns_the_default_when_nothing_matches(self):
        assert receiver._pick({}, "fingerprint", "ruleUid", default="none") == "none"

    def test_treats_none_as_missing(self):
        assert receiver._pick({"fingerprint": None, "ruleUid": "def"}, "fingerprint", "ruleUid") == "def"


# ---- Endpoint --------------------------------------------------------------

class TestHealth:
    def test_health_reports_ok(self, client):
        response = client.get("/health")
        assert response.status_code == 200
        assert response.get_json()["status"] == "ok"


class TestAuth:
    def test_wrong_token_is_rejected(self, client, stored_rows):
        response = _post(client, {"alerts": [{"status": "firing"}]}, token="wrong")
        assert response.status_code == 401
        assert stored_rows == []

    def test_missing_token_is_rejected(self, client, stored_rows):
        response = client.post("/grafana", json={"alerts": []})
        assert response.status_code == 401
        assert stored_rows == []


class TestPayloadHandling:
    def test_non_object_json_is_a_client_error(self, client, stored_rows):
        response = _post(client, ["not", "an", "object"])
        assert response.status_code == 400
        assert stored_rows == []

    def test_one_row_per_alert_is_stored(self, client, stored_rows):
        payload = {"alerts": [{"status": "firing"}, {"status": "resolved"}]}
        response = _post(client, payload)
        assert response.status_code == 200
        assert response.get_json() == {"ok": True, "stored": 2}
        assert len(stored_rows) == 2

    def test_alert_fields_are_extracted_into_the_row(self, client, stored_rows):
        payload = {
            "orgId": 1,
            "panelId": 7,
            "dashboardUID": "dash-1",
            "alerts": [{
                "status": "firing",
                "fingerprint": "fp-1",
                "labels": {"severity": "critical"},
                "annotations": {"summary": "No new ingested rows"},
            }],
        }
        response = _post(client, payload)
        assert response.status_code == 200

        source, severity, uid, title, state, dashboard_uid, panel_id, org_id, raw = stored_rows[0]
        assert (source, severity, uid) == ("grafana", "critical", "fp-1")
        assert (title, state) == ("No new ingested rows", "firing")
        assert (dashboard_uid, panel_id, org_id) == ("dash-1", 7, 1)
        assert "No new ingested rows" in raw

    def test_older_grafana_field_names_are_accepted(self, client, stored_rows):
        payload = {
            "dashboardUid": "dash-2",
            "alerts": [{"state": "alerting", "ruleUid": "rule-1", "title": "Kafka lag"}],
        }
        response = _post(client, payload)
        assert response.status_code == 200

        _, severity, uid, title, state, dashboard_uid, _, _, _ = stored_rows[0]
        assert (severity, uid, title, state, dashboard_uid) == (None, "rule-1", "Kafka lag", "alerting", "dash-2")

    def test_non_integer_panel_and_org_ids_are_discarded(self, client, stored_rows):
        payload = {"orgId": "1", "panelId": "7", "alerts": [{"status": "firing"}]}
        response = _post(client, payload)
        assert response.status_code == 200

        *_, panel_id, org_id, _ = stored_rows[0]
        assert (panel_id, org_id) == (None, None)


class TestDatabaseFailure:
    def test_insert_failure_returns_500_so_grafana_retries(self, client, monkeypatch):
        def _boom():
            raise RuntimeError("connection refused")

        monkeypatch.setattr(receiver, "_connect", _boom)
        response = _post(client, {"alerts": [{"status": "firing"}]})
        assert response.status_code == 500
        assert "db_insert_failed" in response.get_json()["error"]
