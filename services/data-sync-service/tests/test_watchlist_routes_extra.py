"""watchlist_routes API coverage."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from data_sync_service.api.watchlist_routes import (
    _backfill_names,
    _to_ts_code,
)
from data_sync_service.main import app

client = TestClient(app)


@pytest.fixture(autouse=True)
def _patch_deps(monkeypatch):
    import data_sync_service.api.watchlist_routes as wr

    monkeypatch.setattr(wr, "list_registry", lambda: [
        {"symbol": "CN:600000", "name": "浦发", "color": "red"},
        {"symbol": "HK:00700", "name": None},
    ])
    monkeypatch.setattr(wr, "upsert_registry", lambda items: len(items))
    monkeypatch.setattr(wr, "get_automation_pending", lambda td: None)
    monkeypatch.setattr(wr, "get_automation_latest", lambda: None)
    monkeypatch.setattr(wr, "get_automation_runs", lambda limit=10: [{"run_id": "r1"}])
    monkeypatch.setattr(wr, "run_watchlist_automation", lambda trigger, force=False: {"ok": True})
    monkeypatch.setattr(wr, "list_fallback_universe_symbols", lambda max_total=80: {"symbols": ["CN:600000"]})
    monkeypatch.setattr(wr, "get_automation_run", lambda rid: None)
    monkeypatch.setattr(wr, "ack_automation_run", lambda rid, screener_added=None, funnel=None: None)
    yield


def test_get_registry() -> None:
    r = client.get("/watchlist/registry")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True and body["count"] == 2


def test_get_registry_error(monkeypatch) -> None:
    import data_sync_service.api.watchlist_routes as wr

    monkeypatch.setattr(wr, "list_registry", lambda: (_ for _ in ()).throw(RuntimeError("db down")))
    r = client.get("/watchlist/registry")
    assert r.status_code == 500


def test_post_registry() -> None:
    r = client.post("/watchlist/registry", json={"items": [{"symbol": "CN:600000", "name": "浦发"}]})
    assert r.status_code == 200
    assert r.json()["count"] == 1


def test_post_registry_error(monkeypatch) -> None:
    import data_sync_service.api.watchlist_routes as wr

    monkeypatch.setattr(wr, "upsert_registry", lambda items: (_ for _ in ()).throw(ValueError("bad")))
    r = client.post("/watchlist/registry", json={"items": [{"symbol": "CN:600000"}]})
    assert r.status_code == 500


def test_backfill_names_no_op() -> None:
    assert _backfill_names([]) == []
    items = [{"symbol": "CN:600000", "name": "已有"}]
    assert _backfill_names(items) == items


def test_backfill_names_fills(monkeypatch) -> None:
    import data_sync_service.api.watchlist_routes as wr

    class Cur:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return None

        def execute(self, sql, params=None):
            return self

        def fetchall(self):
            return [("600000.SH", "浦发银行")]

    class Conn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return None

        def cursor(self):
            return Cur()

    from data_sync_service import db as dblib

    monkeypatch.setattr(wr, "ensure_sb", lambda: None) if hasattr(wr, "ensure_sb") else None
    monkeypatch.setattr(dblib, "get_connection", lambda: Conn())
    out = _backfill_names([{"symbol": "CN:600000", "name": None}])
    assert out[0]["name"] == "浦发银行"


def test_backfill_names_db_error(monkeypatch) -> None:
    from data_sync_service import db as dblib

    monkeypatch.setattr(dblib, "get_connection", lambda: (_ for _ in ()).throw(RuntimeError("down")))
    items = [{"symbol": "CN:600000", "name": None}]
    assert _backfill_names(items) == items


def test_to_ts_code() -> None:
    assert _to_ts_code("CN:600000") == "600000.SH"
    assert _to_ts_code("CN:000001") == "000001.SZ"
    assert _to_ts_code("HK:700") == "00700.HK"
    assert _to_ts_code("ETF:510300") == "510300.SH"
    assert _to_ts_code("ETF:159915") == "159915.SZ"
    assert _to_ts_code("CN:12345") == ""
    assert _to_ts_code("bad") == ""


def test_backfill_registry_names(monkeypatch) -> None:
    import data_sync_service.api.watchlist_routes as wr

    monkeypatch.setattr(wr, "_backfill_names", lambda items: [dict(x, name="浦发银行") for x in items])
    monkeypatch.setattr(wr, "upsert_registry", lambda items: len(items))
    r = client.post("/watchlist/registry/backfill-names")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True and body["filledBefore"] == 1 and body["updatedCount"] == 1


def test_automation_pending() -> None:
    r = client.get("/watchlist/automation/pending")
    assert r.status_code == 200
    assert r.json() == {"pending": False}
    r2 = client.get("/watchlist/automation/pending", params={"tradeDate": "2026-08-07"})
    assert r2.status_code == 200


def test_automation_pending_true(monkeypatch) -> None:
    import data_sync_service.api.watchlist_routes as wr

    monkeypatch.setattr(wr, "get_automation_pending", lambda td: {"runId": "r9", "tradeDate": "2026-08-07"})
    r = client.get("/watchlist/automation/pending")
    body = r.json()
    assert body["pending"] is True and body["runId"] == "r9"


def test_automation_latest() -> None:
    r = client.get("/watchlist/automation/latest")
    assert r.json() == {"found": False}


def test_automation_latest_found(monkeypatch) -> None:
    import data_sync_service.api.watchlist_routes as wr

    monkeypatch.setattr(wr, "get_automation_latest", lambda: {"runId": "r2"})
    r = client.get("/watchlist/automation/latest")
    body = r.json()
    assert body["found"] is True and body["runId"] == "r2"


def test_automation_runs() -> None:
    r = client.get("/watchlist/automation/runs")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True and body["runs"] == [{"run_id": "r1"}]
    assert "asOfDate" in body
    r2 = client.get("/watchlist/automation/runs", params={"limit": 99})
    assert r2.status_code == 422  # le=30


def test_automation_run_manual() -> None:
    r = client.post("/watchlist/automation/run")
    assert r.status_code == 200
    assert r.json() == {"ok": True}
    r2 = client.post("/watchlist/automation/run?force=true")
    assert r2.status_code == 200


def test_automation_run_error(monkeypatch) -> None:
    import data_sync_service.api.watchlist_routes as wr

    monkeypatch.setattr(wr, "run_watchlist_automation", lambda trigger, force=False: (_ for _ in ()).throw(RuntimeError("x")))
    assert client.post("/watchlist/automation/run").status_code == 500


def test_fallback_universe() -> None:
    r = client.get("/watchlist/automation/fallback-universe")
    assert r.status_code == 200
    assert r.json()["symbols"] == ["CN:600000"]
    r2 = client.get("/watchlist/automation/fallback-universe", params={"maxTotal": 300})
    assert r2.status_code == 422


def test_automation_ack_not_found() -> None:
    r = client.post("/watchlist/automation/r1/ack")
    assert r.status_code == 404
    assert r.json()["detail"] == "run not found"


def test_automation_ack_found(monkeypatch) -> None:
    import data_sync_service.api.watchlist_routes as wr

    monkeypatch.setattr(wr, "ack_automation_run", lambda rid, screener_added=None, funnel=None: {"runId": rid})
    r = client.post("/watchlist/automation/r1/ack", json={"screenerAdded": 3, "funnel": {"a": 1}})
    assert r.status_code == 200
    assert r.json()["runId"] == "r1"


def test_automation_get_not_found() -> None:
    r = client.get("/watchlist/automation/r1")
    assert r.status_code == 404


def test_automation_get_found(monkeypatch) -> None:
    import data_sync_service.api.watchlist_routes as wr

    monkeypatch.setattr(wr, "get_automation_run", lambda rid: {"runId": rid, "status": "done"})
    r = client.get("/watchlist/automation/r1")
    assert r.status_code == 200
    assert r.json()["status"] == "done"
