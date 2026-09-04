"""api/news_routes.py coverage via TestClient."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from data_sync_service.main import app

client = TestClient(app)

import data_sync_service.api.news_routes as nr  # noqa: E402


@pytest.fixture(autouse=True)
def _patch_db(monkeypatch):
    monkeypatch.setattr(nr, "ensure_tables", lambda: None)
    monkeypatch.setattr(nr, "fetch_sources", lambda enabled_only=False: [{"id": "s1", "name": "X"}])
    monkeypatch.setattr(nr, "create_source", lambda **kw: {"id": kw["source_id"]})
    monkeypatch.setattr(nr, "update_source", lambda **kw: {"id": kw["source_id"]})
    monkeypatch.setattr(nr, "delete_source", lambda sid: True)
    monkeypatch.setattr(nr, "fetch_items", lambda **kw: (3, [{"id": "i1"}]))
    monkeypatch.setattr(nr, "mark_item_read", lambda iid: True)
    monkeypatch.setattr(nr, "mark_item_important", lambda iid, imp: True)
    monkeypatch.setattr(nr, "fetch_all_sources", lambda: [{"ok": True}])
    monkeypatch.setattr(nr, "add_default_sources", lambda: None)
    monkeypatch.setattr(nr, "count_by_enrichment_status", lambda: {"done": 2})
    yield


def test_list_sources() -> None:
    r = client.get("/api/news/sources")
    assert r.status_code == 200 and r.json()["sources"][0]["id"] == "s1"
    r2 = client.get("/api/news/sources", params={"enabled_only": "true"})
    assert r2.status_code == 200


def test_add_source() -> None:
    r = client.post("/api/news/sources", json={"id": "sx", "name": "N", "url": "http://x"})
    assert r.status_code == 200 and r.json()["source"]["id"] == "sx"


def test_add_source_auto_id() -> None:
    r = client.post("/api/news/sources", json={"name": "N", "url": "http://x"})
    body = r.json()
    assert len(body["source"]["id"]) == 8


def test_add_source_invalid() -> None:
    # OPT-142: empty name/url is a 422 (Pydantic), not a 200 + error string.
    r = client.post("/api/news/sources", json={"name": "", "url": ""})
    assert r.status_code == 422


def test_patch_source() -> None:
    r = client.patch("/api/news/sources/s1", json={"name": "N2"})
    assert r.status_code == 200 and r.json()["source"]["id"] == "s1"


def test_patch_source_missing(monkeypatch) -> None:
    monkeypatch.setattr(nr, "update_source", lambda **kw: None)
    r = client.patch("/api/news/sources/nope", json={})
    assert r.json()["error"] == "source not found"


def test_delete_source() -> None:
    r = client.delete("/api/news/sources/s1")
    assert r.status_code == 200 and r.json()["deleted"] is True


def test_list_items() -> None:
    r = client.get("/api/news/items", params={"limit": 10, "source_id": "s1", "is_read": "true"})
    body = r.json()
    assert body["total"] == 3 and body["items"] == [{"id": "i1"}]


def test_mark_read() -> None:
    r = client.post("/api/news/items/i1/read")
    assert r.status_code == 200 and r.json()["updated"] is True


def test_mark_important() -> None:
    r = client.post("/api/news/items/i1/important", json={"important": True})
    assert r.status_code == 200
    r2 = client.post("/api/news/items/i1/important", json={"important": False})
    assert r2.status_code == 200


def test_refresh_feeds() -> None:
    r = client.post("/api/news/refresh")
    assert r.status_code == 200 and r.json()["results"] == [{"ok": True}]


def test_init_defaults() -> None:
    r = client.post("/api/news/init-defaults")
    assert r.status_code == 200 and r.json()["ok"] is True


def test_enrichment_status() -> None:
    r = client.get("/api/news/enrichment/status")
    assert r.status_code == 200 and r.json()["counts"] == {"done": 2}


def test_run_enrichment(monkeypatch) -> None:
    from data_sync_service.service import news_enrich

    monkeypatch.setattr(news_enrich, "run_enrichment_cycle", lambda max_batches=10: {"batches": max_batches})
    r = client.post("/api/news/enrichment/run", params={"max_batches": 3})
    assert r.status_code == 200 and r.json()["batches"] == 3


def test_get_latest_brief(monkeypatch) -> None:
    from data_sync_service.db import morning_brief

    monkeypatch.setattr(morning_brief, "fetch_latest_brief", lambda brief_type=None: {"briefDate": "d"})
    r = client.get("/api/news/brief/latest")
    assert r.status_code == 200 and r.json()["brief"]["briefDate"] == "d"


def test_get_recent_briefs(monkeypatch) -> None:
    from data_sync_service.db import morning_brief

    monkeypatch.setattr(morning_brief, "fetch_recent_briefs", lambda limit=7: [{"briefDate": "d"}])
    r = client.get("/api/news/brief/recent", params={"limit": 3})
    assert r.status_code == 200 and len(r.json()["briefs"]) == 1


def test_generate_brief(monkeypatch) -> None:
    from data_sync_service.service import morning_brief

    monkeypatch.setattr(morning_brief, "generate_brief", lambda brief_type="morning": {"briefType": brief_type})
    r = client.post("/api/news/brief/generate", params={"brief_type": "midday"})
    assert r.status_code == 200 and r.json()["brief"]["briefType"] == "midday"
