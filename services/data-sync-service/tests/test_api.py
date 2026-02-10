from fastapi.testclient import TestClient  # type: ignore[import-not-found]

from data_sync_service.main import app  # type: ignore[import-not-found]


def test_healthz() -> None:
    client = TestClient(app)
    response = client.get("/healthz")
    assert response.status_code == 200
    payload = response.json()
    assert "status" in payload
    assert "db" in payload


def test_market_bars_compat_endpoint_shape() -> None:
    client = TestClient(app)
    resp = client.get("/market/stocks/CN:000001/bars?days=60")
    assert resp.status_code == 200
    payload = resp.json()
    assert set(payload.keys()) >= {"symbol", "market", "ticker", "name", "currency", "bars"}
    assert isinstance(payload["bars"], list)


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


def test_industry_fund_flow_endpoint_shape() -> None:
    client = TestClient(app)
    resp = client.get("/market/cn/industry-fund-flow?days=10&topN=5")
    assert resp.status_code == 200
    payload = resp.json()
    assert set(payload.keys()) >= {"asOfDate", "days", "topN", "dates", "top"}


def test_market_sentiment_endpoint_shape() -> None:
    client = TestClient(app)
    resp = client.get("/market/cn/sentiment?days=5")
    assert resp.status_code == 200
    payload = resp.json()
    assert set(payload.keys()) >= {"asOfDate", "days", "items"}


def test_dashboard_summary_endpoint_shape() -> None:
    client = TestClient(app)
    resp = client.get("/dashboard/summary")
    assert resp.status_code == 200
    payload = resp.json()
    assert set(payload.keys()) >= {"asOfDate", "industryFundFlow", "marketSentiment", "screeners"}
    assert isinstance(payload.get("screeners"), list)
    ind = payload.get("industryFundFlow") or {}
    assert isinstance(ind, dict)
    assert set(ind.keys()) >= {"dates", "topByDate", "flow5d"}
    ms = payload.get("marketSentiment") or {}
    assert isinstance(ms, dict)
    assert "items" in ms and isinstance(ms["items"], list)


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


def test_global_stock_search_endpoint_shape() -> None:
    client = TestClient(app)
    resp = client.get("/search/stocks?limit=8&q=000001")
    assert resp.status_code == 200
    payload = resp.json()
    assert "items" in payload
    assert isinstance(payload["items"], list)
