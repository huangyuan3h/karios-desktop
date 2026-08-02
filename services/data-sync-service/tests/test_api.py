import pytest
from fastapi.testclient import TestClient  # type: ignore[import-not-found]

from data_sync_service.main import app  # type: ignore[import-not-found]


def test_healthz() -> None:
    client = TestClient(app)
    response = client.get("/healthz")
    assert response.status_code == 200
    payload = response.json()
    assert "status" in payload
    assert "db" in payload


@pytest.mark.requires_postgres
def test_market_bars_compat_endpoint_shape() -> None:
    client = TestClient(app)
    resp = client.get("/market/stocks/CN:000001/bars?days=60")
    assert resp.status_code == 200
    payload = resp.json()
    assert set(payload.keys()) >= {"symbol", "market", "ticker", "name", "currency", "bars"}
    assert isinstance(payload["bars"], list)


@pytest.mark.requires_postgres
def test_market_bars_force_triggers_symbol_sync() -> None:
    from unittest.mock import patch

    client = TestClient(app)
    with patch("data_sync_service.service.market_bars.sync_daily_for_ts_code") as mock_sync:
        mock_sync.return_value = {"ok": True, "updated": 0, "skipped": True}
        resp = client.get("/market/stocks/CN:000001/bars?days=60&force=true")
    assert resp.status_code == 200
    mock_sync.assert_called_once_with("000001.SZ")


@pytest.mark.requires_postgres
def test_trendok_endpoint_shape() -> None:
    client = TestClient(app)
    resp = client.get("/market/stocks/trendok?symbols=CN:000001")
    assert resp.status_code == 200
    arr = resp.json()
    assert isinstance(arr, list)
    assert len(arr) == 1
    assert arr[0]["symbol"] == "CN:000001"
    assert set(arr[0].keys()) >= {
        "symbol",
        "name",
        "asOfDate",
        "trendOk",
        "score",
        "scoreParts",
        "stopLossPrice",
        "stopLossParts",
        "buyMode",
        "buyAction",
        "buyZoneLow",
        "buyZoneHigh",
        "buyRefPrice",
        "buyWhy",
        "buyChecks",
        "marketRegime",
        "rs",
        "intradayChgPct",
        "gapUp",
        "riskMetricsLive",
        "riskAlerts",
        "instFlow",
        "checks",
        "values",
        "missingData",
    }
    assert isinstance(arr[0]["scoreParts"], dict)
    assert isinstance(arr[0]["stopLossParts"], dict)
    assert isinstance(arr[0]["buyChecks"], dict)
    assert isinstance(arr[0]["checks"], dict)
    assert isinstance(arr[0]["values"], dict)
    assert isinstance(arr[0]["missingData"], list)
    assert isinstance(arr[0]["riskAlerts"], list)


@pytest.mark.requires_postgres
def test_tv_screeners_endpoint_shape() -> None:
    client = TestClient(app)
    resp = client.get("/integrations/tradingview/screeners")
    assert resp.status_code == 200
    payload = resp.json()
    assert "items" in payload and isinstance(payload["items"], list)
    # Defaults should exist on a fresh DB.
    ids = {x.get("id") for x in payload["items"] if isinstance(x, dict)}
    assert {"falcon", "blackhorse"}.issubset(ids)


def test_tv_chrome_status_endpoint_shape() -> None:
    client = TestClient(app)
    resp = client.get("/integrations/tradingview/status")
    assert resp.status_code == 200
    payload = resp.json()
    assert set(payload.keys()) >= {
        "running",
        "pid",
        "host",
        "port",
        "cdpOk",
        "cdpVersion",
        "userDataDir",
        "profileDirectory",
        "headless",
    }


@pytest.mark.requires_postgres
def test_broker_accounts_state_shape() -> None:
    client = TestClient(app)
    created = client.post(
        "/broker/accounts",
        json={"broker": "pingan", "title": "Test Account", "accountMasked": "1234****5678"},
    )
    assert created.status_code == 200
    acc = created.json()
    assert set(acc.keys()) >= {"id", "broker", "title", "accountMasked", "updatedAt"}

    state_resp = client.get(f"/broker/pingan/accounts/{acc['id']}/state")
    assert state_resp.status_code == 200
    state = state_resp.json()
    assert set(state.keys()) >= {
        "accountId",
        "broker",
        "updatedAt",
        "overview",
        "positions",
        "conditionalOrders",
        "trades",
        "counts",
    }
    assert isinstance(state["positions"], list)
    assert isinstance(state["conditionalOrders"], list)
    assert isinstance(state["trades"], list)


@pytest.mark.requires_postgres
def test_industry_fund_flow_endpoint_shape() -> None:
    client = TestClient(app)
    resp = client.get("/market/cn/industry-fund-flow?days=10&topN=5")
    assert resp.status_code == 200
    payload = resp.json()
    assert set(payload.keys()) >= {"asOfDate", "days", "topN", "dates", "top"}


@pytest.mark.requires_postgres
def test_market_sentiment_endpoint_shape() -> None:
    client = TestClient(app)
    resp = client.get("/market/cn/sentiment?days=5")
    assert resp.status_code == 200
    payload = resp.json()
    assert set(payload.keys()) >= {"asOfDate", "days", "items"}


@pytest.mark.requires_postgres
def test_dashboard_summary_endpoint_shape() -> None:
    client = TestClient(app)
    resp = client.get("/dashboard/summary")
    assert resp.status_code == 200
    payload = resp.json()
    assert set(payload.keys()) >= {
        "asOfDate",
        "industryFundFlow",
        "marketSentiment",
        "screeners",
        "marketEnvironmentZh",
    }
    assert isinstance(payload.get("screeners"), list)
    ind = payload.get("industryFundFlow") or {}
    assert isinstance(ind, dict)
    assert set(ind.keys()) >= {"dates", "topByDate", "flow5d"}
    ms = payload.get("marketSentiment") or {}
    assert isinstance(ms, dict)
    assert "items" in ms and isinstance(ms["items"], list)


def test_tv_screener_sync_returns_202_with_job(monkeypatch) -> None:
    import data_sync_service.service.tv as tvsvc  # type: ignore[import-not-found]

    monkeypatch.setattr(
        tvsvc,
        "enqueue_screener_capture",
        lambda *, screener_id, trigger="api": {
            "jobId": "job-123",
            "screenerId": screener_id,
            "status": "queued",
        },
    )
    client = TestClient(app)
    resp = client.post("/integrations/tradingview/screeners/falcon/sync")
    assert resp.status_code == 202
    payload = resp.json()
    assert payload["jobId"] == "job-123"
    assert payload["screenerId"] == "falcon"
    assert payload["status"] == "queued"


def test_tv_capture_job_get_endpoint(monkeypatch) -> None:
    import data_sync_service.service.tv as tvsvc  # type: ignore[import-not-found]

    monkeypatch.setattr(
        tvsvc,
        "get_capture_job",
        lambda job_id: {
            "jobId": job_id,
            "screenerId": "falcon",
            "status": "done",
            "rowCount": 12,
            "snapshotId": "snap-1",
        },
    )
    client = TestClient(app)
    resp = client.get("/integrations/tradingview/capture-jobs/job-123")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["status"] == "done"
    assert payload["rowCount"] == 12


@pytest.mark.requires_postgres
def test_dashboard_sync_endpoint_shape() -> None:
    client = TestClient(app)
    # Avoid running TradingView sync in tests (it may require Chrome profile/login).
    resp = client.post("/dashboard/sync?force=true&screeners=false", json={})
    assert resp.status_code == 200
    payload = resp.json()
    assert set(payload.keys()) >= {"ok", "startedAt", "finishedAt", "steps", "screener"}
    assert isinstance(payload.get("steps"), list)
    assert isinstance(payload.get("screener"), dict)


def test_market_chips_cn_only(monkeypatch) -> None:
    import data_sync_service.service.market_detail as market_detail  # type: ignore[import-not-found]

    monkeypatch.setattr(
        market_detail,
        "fetch_cn_a_chip_summary",
        lambda ticker, days=60: [
            {
                "date": "2025-12-20",
                "profitRatio": "0.5",
                "avgCost": "10.0",
                "cost90Low": "9.0",
                "cost90High": "11.0",
                "cost90Conc": "0.2",
                "cost70Low": "9.5",
                "cost70High": "10.5",
                "cost70Conc": "0.1",
            }
        ],
    )
    client = TestClient(app)
    resp = client.get("/market/stocks/CN:000001/chips?days=60&force=true")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["symbol"] == "CN:000001"
    assert payload["market"] == "CN"
    assert payload["items"][0]["avgCost"] == "10.0"

    resp = client.get("/market/stocks/HK:00005/chips?days=60")
    assert resp.status_code == 400


def test_market_fund_flow_cn_only(monkeypatch) -> None:
    import data_sync_service.service.market_detail as market_detail  # type: ignore[import-not-found]

    monkeypatch.setattr(
        market_detail,
        "fetch_cn_a_fund_flow",
        lambda ticker, days=60: [
            {
                "date": "2025-12-20",
                "close": "10.0",
                "changePct": "1.0",
                "mainNetAmount": "100",
                "mainNetRatio": "2.0",
                "superNetAmount": "40",
                "superNetRatio": "1.0",
                "largeNetAmount": "30",
                "largeNetRatio": "0.8",
                "mediumNetAmount": "20",
                "mediumNetRatio": "0.5",
                "smallNetAmount": "10",
                "smallNetRatio": "0.2",
            }
        ],
    )
    client = TestClient(app)
    resp = client.get("/market/stocks/CN:000001/fund-flow?days=60&force=true")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["symbol"] == "CN:000001"
    assert payload["items"][0]["mainNetAmount"] == "100"

    resp = client.get("/market/stocks/HK:00005/fund-flow?days=60")
    assert resp.status_code == 400


@pytest.mark.requires_postgres
def test_global_stock_search_endpoint_shape() -> None:
    client = TestClient(app)
    resp = client.get("/search/stocks?limit=8&q=000001")
    assert resp.status_code == 200
    payload = resp.json()
    assert "items" in payload
    assert isinstance(payload["items"], list)


@pytest.mark.requires_postgres
def test_system_prompt_endpoints_shape_and_roundtrip() -> None:
    client = TestClient(app)

    # Snapshot previous state (best-effort restore).
    prev_active = client.get("/system-prompts/active").json()
    prev_legacy = client.get("/settings/system-prompt").json().get("value", "")

    created_id: str | None = None
    try:
        # Create preset (becomes active by default).
        created = client.post("/system-prompts", json={"title": "Test Prompt", "content": "Hello"})
        assert created.status_code == 200
        created_id = created.json().get("id")
        assert isinstance(created_id, str) and created_id

        # Active should be this preset.
        active = client.get("/system-prompts/active")
        assert active.status_code == 200
        a = active.json()
        assert a.get("id") == created_id
        assert isinstance(a.get("title"), str)
        assert isinstance(a.get("content"), str)

        # Legacy PUT should update active preset content when active exists.
        resp = client.put("/settings/system-prompt", json={"value": "Updated via legacy"})
        assert resp.status_code == 200
        assert resp.json().get("ok") is True

        active2 = client.get("/system-prompts/active").json()
        assert active2.get("id") == created_id
        assert active2.get("content") == "Updated via legacy"
    finally:
        # Restore active selection
        if prev_active.get("id"):
            client.put("/system-prompts/active", json={"id": prev_active.get("id")})
        else:
            client.put("/system-prompts/active", json={"id": None})
            client.put("/settings/system-prompt", json={"value": str(prev_legacy or "")})

        # Cleanup created preset
        if created_id:
            client.delete(f"/system-prompts/{created_id}")


@pytest.mark.requires_postgres
def test_alpha_radar_endpoints_shape() -> None:
    client = TestClient(app)
    init = client.post("/api/alpha-radar/init-defaults")
    assert init.status_code == 200
    assert init.json().get("ok") is True

    sources = client.get("/api/alpha-radar/sources")
    assert sources.status_code == 200
    assert isinstance(sources.json().get("sources"), list)

    docs = client.get("/api/alpha-radar/documents?limit=5")
    assert docs.status_code == 200
    payload = docs.json()
    assert "total" in payload
    assert isinstance(payload.get("items"), list)

    trends = client.get("/api/alpha-radar/trends?limit=5")
    assert trends.status_code == 200
    trend_items = trends.json().get("items")
    assert isinstance(trend_items, list)
    if trend_items:
        sample = trend_items[0]
        assert "macroTheme" in sample
        assert "catalystGrade" in sample

    catalyst = client.get("/api/alpha-radar/catalyst-stocks?limit=5")
    assert catalyst.status_code == 200
    catalyst_body = catalyst.json()
    assert catalyst_body.get("stalenessBasis") == "published_then_fetched"
    assert "maxAgeDays" in catalyst_body
    assert isinstance(catalyst_body.get("items"), list)

    status = client.get("/api/alpha-radar/status")
    assert status.status_code == 200
    body = status.json()
    assert "lastRunAt" in body
    assert "cooldownHours" in body
    assert "jobType" in body

    missing = client.delete("/api/alpha-radar/trends/nonexistent-trend-id")
    assert missing.status_code == 200


@pytest.mark.requires_postgres
def test_sync_jobs_aggregate_endpoint_shape() -> None:
    """GET /sync/jobs returns per-job today_run/last_success plus extras."""
    client = TestClient(app)
    resp = client.get("/sync/jobs")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload.get("ok") is True
    assert isinstance(payload.get("jobs"), dict)

    # All known tracked job_types must be present, even if no run yet.
    expected_job_types = {
        "stock_basic_sync",
        "hk_basic_sync",
        "hk_daily_full",
        "hk_industry_sync",
        "etf_fund_basic_sync",
        "etf_daily_full",
        "stock_daily_full",
        "stock_adj_factor_full",
        "stock_close_sync",
        "stock_close_catchup",
        "index_daily_full",
        "macro_daily_full",
        "eastmoney_industry_sync",
        "alpha_radar_pipeline",
        "alpha_radar_ingest",
        "alpha_radar_process",
        "watchlist_automation",
        "news_fetch_job",
    }
    assert expected_job_types.issubset(set(payload["jobs"].keys()))

    for job_type, entry in payload["jobs"].items():
        assert set(entry.keys()) == {"todayRun", "lastSuccess"}, job_type
        assert entry["todayRun"] is None or isinstance(entry["todayRun"], dict), job_type
        assert entry["lastSuccess"] is None or isinstance(entry["lastSuccess"], dict), job_type
        if entry["todayRun"]:
            assert set(entry["todayRun"].keys()) >= {
                "id",
                "job_type",
                "sync_at",
                "success",
            }
            assert entry["todayRun"]["job_type"] == job_type

    # HK industry coverage, alpha radar, watchlist automation are optional
    # best-effort - they may be null on a fresh DB without crashing the endpoint.
    assert "hkIndustryCoverage" in payload
    assert "alphaRadar" in payload
    assert "watchlistAutomation" in payload
