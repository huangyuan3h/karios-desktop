"""alpha_radar_routes API coverage."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from data_sync_service.main import app

client = TestClient(app)

import data_sync_service.api.alpha_radar_routes as ar  # noqa: E402


@pytest.fixture(autouse=True)
def _patch_deps(monkeypatch):
    monkeypatch.setattr(ar, "ensure_tables", lambda: None)
    monkeypatch.setattr(ar, "fetch_sources", lambda enabled_only=True, category=None: [{"id": "s1"}])
    monkeypatch.setattr(ar, "add_default_sources", lambda: 3)
    monkeypatch.setattr(ar, "fetch_documents", lambda **kw: (2, [{"id": "d1"}]))
    monkeypatch.setattr(ar, "pipeline_status", lambda: {"phase": "idle"})
    monkeypatch.setattr(ar, "run_alpha_radar_ingest", lambda trigger, force_reprocess=False: {"ingested": 1})
    monkeypatch.setattr(ar, "run_alpha_radar_process", lambda trigger, max_rounds=None: {"processed": 1})
    monkeypatch.setattr(ar, "run_alpha_radar_pipeline", lambda force=False, trigger="manual": {"done": True})
    monkeypatch.setattr(ar, "list_catalyst_stocks", lambda limit=50, max_age_days=None: [{"symbol": "x"}])
    monkeypatch.setattr(ar, "get_auto_qa_stats", lambda since_days=7, limit=20: {"penalties": []})
    monkeypatch.setattr(ar, "get_meta", lambda key: "2026-08-07T10:00:00")
    monkeypatch.setattr(ar, "fetch_trends", lambda **kw: (1, [{"id": "t1"}]))
    monkeypatch.setattr(ar, "fetch_all_sources", lambda enrich_fulltext=None, apply_filter=True, force_reprocess=False: {"fetched": 5})
    monkeypatch.setattr(ar, "process_document", lambda doc_id, map_cn=True: {"processed": 1})
    monkeypatch.setattr(ar, "process_pending_documents", lambda limit=10, map_cn=True, mode="batch": {"processed": 2})
    monkeypatch.setattr(ar, "fetch_trend_by_id", lambda tid: {"id": tid, "keywordsForMapping": ["芯片"]} if tid == "t1" else None)
    monkeypatch.setattr(ar, "get_cn_industry_mainline", lambda: {"currentMainline": [{"industryName": "半导体"}]})
    monkeypatch.setattr(ar, "build_mainline_score_map", lambda m: {"半导体": 90.0})
    monkeypatch.setattr(ar, "compute_risk_status", lambda **kw: {"level": "low"})
    monkeypatch.setattr(ar, "update_trend_risk_status", lambda tid, rs: None)
    monkeypatch.setattr(ar, "remap_trend_by_id", lambda tid: {"cnSymbols": ["600000.SH"]})
    monkeypatch.setattr(ar, "delete_trend_by_id", lambda tid: True)
    yield


def test_list_sources() -> None:
    r = client.get("/api/alpha-radar/sources")
    assert r.status_code == 200
    assert r.json()["sources"] == [{"id": "s1"}]
    r2 = client.get("/api/alpha-radar/sources", params={"enabled_only": "false", "category": "news"})
    assert r2.status_code == 200


def test_init_defaults() -> None:
    r = client.post("/api/alpha-radar/init-defaults")
    assert r.json() == {"ok": True}


def test_list_documents() -> None:
    r = client.get("/api/alpha-radar/documents", params={"limit": 20, "hours": 24})
    assert r.status_code == 200
    assert r.json()["total"] == 2


def test_status() -> None:
    r = client.get("/api/alpha-radar/status")
    assert r.json()["ok"] is True and r.json()["phase"] == "idle"


def test_run_ingest() -> None:
    r = client.post("/api/alpha-radar/run-ingest", json={"forceReprocess": True})
    assert r.json()["ok"] is True and r.json()["ingested"] == 1
    r2 = client.post("/api/alpha-radar/run-ingest")
    assert r2.status_code == 200


def test_run_process() -> None:
    r = client.post("/api/alpha-radar/run-process", json={"maxRounds": 3})
    assert r.json()["processed"] == 1
    client.post("/api/alpha-radar/run-process", json={})


def test_run_pipeline() -> None:
    r = client.post("/api/alpha-radar/run-pipeline", json={"force": True})
    assert r.json()["done"] is True


def test_generate_daily_alias() -> None:
    r = client.post("/api/alpha-radar/generate-daily")
    assert r.status_code == 200 and r.json()["done"] is True


def test_catalyst_stocks() -> None:
    r = client.get("/api/alpha-radar/catalyst-stocks", params={"maxAgeDays": 7})
    assert r.status_code == 200 and r.json() == [{"symbol": "x"}]


def test_auto_qa_stats() -> None:
    r = client.get("/api/alpha-radar/auto-qa-stats", params={"sinceDays": 14, "limit": 5})
    assert r.status_code == 200 and r.json() == {"penalties": []}


def test_list_trends_defaults() -> None:
    r = client.get("/api/alpha-radar/trends")
    assert r.status_code == 200
    assert r.json()["since"] == "2026-08-07T10:00:00"  # latest_batch → last_batch_started_at


def test_list_trends_explicit_filters() -> None:
    r = client.get("/api/alpha-radar/trends", params={
        "limit": 10, "offset": 5, "day": "all", "risk_status": "low",
        "since": "2026-08-01", "latest_batch": "false", "maxAgeDays": 30,
    })
    body = r.json()
    assert body["day"] is None  # "all" normalized
    assert body["since"] == "2026-08-01" and body["maxAgeDays"] == 30


def test_sync_feeds() -> None:
    r = client.post("/api/alpha-radar/sync", json={"enrichFulltext": True, "applyFilter": False})
    assert r.json()["ok"] is True and r.json()["fetched"] == 5
    client.post("/api/alpha-radar/sync", json={})


def test_process_feeds_batch() -> None:
    r = client.post("/api/alpha-radar/process", json={"limit": 5, "mapCn": False, "mode": "single"})
    assert r.status_code == 200 and r.json()["processed"] == 2


def test_process_feeds_document() -> None:
    r = client.post("/api/alpha-radar/process", json={"documentId": "doc1"})
    assert r.json()["ok"] is True and r.json()["processed"] == 1


def test_refresh_trend_risk() -> None:
    r = client.post("/api/alpha-radar/trends/t1/refresh-risk")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True and body["trendId"] == "t1"
    assert body["riskStatus"] == {"level": "low"}


def test_refresh_trend_risk_not_found() -> None:
    r = client.post("/api/alpha-radar/trends/nope/refresh-risk")
    assert r.status_code == 200
    assert r.json()["error"] == "trend not found"


def test_remap_trend() -> None:
    r = client.post("/api/alpha-radar/trends/t1/remap")
    assert r.status_code == 200
    assert r.json()["ok"] is True and r.json()["cnSymbols"] == ["600000.SH"]


def test_remap_trend_value_error(monkeypatch) -> None:
    monkeypatch.setattr(ar, "remap_trend_by_id", lambda tid: (_ for _ in ()).throw(ValueError("trend not found: x")))
    r = client.post("/api/alpha-radar/trends/nope/remap")
    assert r.status_code == 200
    assert r.json()["ok"] is False and "trend not found" in r.json()["error"]


def test_delete_trend() -> None:
    r = client.delete("/api/alpha-radar/trends/t1")
    assert r.json()["ok"] is True and r.json()["trendId"] == "t1"


def test_delete_trend_not_found() -> None:
    r = client.delete("/api/alpha-radar/trends/nope")
    assert r.json()["ok"] is False and r.json()["error"] == "trend not found"
