"""Health/datasource freshness endpoints (TIP-013)."""

from __future__ import annotations

from fastapi.testclient import TestClient

from data_sync_service.main import app

client = TestClient(app)


def test_datasources_endpoint_shape() -> None:
    resp = client.get("/api/health/datasources")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["ok"] is True
    sources = {s["source"]: s for s in payload["sources"]}
    assert {"market", "news", "research", "watchlist", "macro", "alpha_radar"} <= set(sources)
    for source in sources.values():
        assert "label" in source
        assert "ageMinutes" in source
        assert "thresholdMinutes" in source
        assert "stale" in source
    assert sources["news"]["stale"] in (True, False)
    assert sources["news"]["thresholdMinutes"] == 360
