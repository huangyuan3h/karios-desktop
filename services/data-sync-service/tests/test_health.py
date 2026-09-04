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
    assert "twin_star_etf" in sources
    assert "twin_star_intraday" in sources
    assert sources["daily_basic"]["label"].startswith("双子星")
    assert sources["twin_star_etf"]["label"].startswith("双子星")
    assert sources["twin_star_intraday"]["label"].startswith("双子星")


def test_twin_star_etf_source_matches_sleeve_codes() -> None:
    from data_sync_service.api.health_routes import _SOURCES
    from data_sync_service.service.etf_daily import SLEEVE_ETF_TS_CODES

    spec = next(s for s in _SOURCES if s["source"] == "twin_star_etf")
    for code in SLEEVE_ETF_TS_CODES:
        assert code in spec["whereSql"]
    assert spec["jobType"] == "sleeve_etf_daily_sync"
