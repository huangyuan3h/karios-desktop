from datetime import date, timedelta

from fastapi.testclient import TestClient

import main


def test_strategy_prompt_and_daily_report(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.sqlite3"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    client = TestClient(main.app)
    acc = client.post(
        "/broker/accounts",
        json={"broker": "pingan", "title": "Main", "accountMasked": "3260****7775"},
    ).json()
    account_id = acc["id"]

    # Prompt CRUD
    resp = client.put(
        f"/strategy/accounts/{account_id}/prompt",
        json={"prompt": "No margin. Max 3 positions. CN/HK only."},
    )
    assert resp.status_code == 200
    assert resp.json()["prompt"].startswith("No margin")

    resp = client.get(f"/strategy/accounts/{account_id}/prompt")
    assert resp.status_code == 200
    assert "Max 3 positions" in resp.json()["prompt"]

    # Seed a TradingView snapshot for falcon screener so candidate pool is not empty.
    with main._connect() as conn:
        # Ensure default screeners exist.
        main._seed_default_tv_screeners()
        payload = {
            "screenTitle": "Falcon",
            "filters": ["TestFilter"],
            "url": "https://www.tradingview.com/screener/falcon/",
            "headers": ["Symbol", "Price"],
            "rows": [
                {"Symbol": "300502\nXin Yi Sheng\nD", "Price": "434.32 CNY"},
                {"Symbol": "300308\nZhong Ji Xuchuang\nD", "Price": "573.74 CNY"},
            ],
        }
        conn.execute(
            """
            INSERT INTO tv_screener_snapshots(id, screener_id, captured_at, row_count, headers_json, rows_json)
            VALUES(?, ?, ?, ?, ?, ?)
            """,
            (
                "snap-1",
                "falcon",
                "2025-12-21T00:00:00Z",
                2,
                '["Symbol","Price"]',
                main.json.dumps(payload, ensure_ascii=False),
            ),
        )
        conn.commit()

    # Seed one sentiment day so includeMarketSentiment can inject data.
    main._upsert_cn_sentiment_daily(
        date="2025-12-21",
        as_of_date="2025-12-21",
        up=500,
        down=4000,
        flat=100,
        up_down_ratio=0.125,
        premium=-0.5,
        failed_rate=35.0,
        risk_mode="no_new_positions",
        rules=["premium<0 && failedLimitUpRate>30 => no_new_positions"],
        updated_at="2025-12-21T00:00:00Z",
        raw={},
    )

    # Avoid AkShare and AI calls in tests.
    def fake_bars(symbol: str, days: int = 60, force: bool = False):
        return main.MarketBarsResponse(
            symbol=symbol,
            market="CN",
            ticker=symbol.split(":")[1],
            name="Test",
            currency="CNY",
            bars=[
                {"date": "2025-12-20", "open": "10", "high": "11", "low": "9", "close": "10", "volume": "100", "amount": "1000"},
                {"date": "2025-12-21", "open": "10", "high": "12", "low": "10", "close": "11", "volume": "120", "amount": "1200"},
            ],
        )

    def fake_chips(symbol: str, days: int = 30, force: bool = False):
        return main.MarketChipsResponse(
            symbol=symbol,
            market="CN",
            ticker=symbol.split(":")[1],
            name="Test",
            currency="CNY",
            items=[],
        )

    def fake_flow(symbol: str, days: int = 30, force: bool = False):
        return main.MarketFundFlowResponse(
            symbol=symbol,
            market="CN",
            ticker=symbol.split(":")[1],
            name="Test",
            currency="CNY",
            items=[],
        )

    monkeypatch.setattr(main, "market_stock_bars", fake_bars)
    monkeypatch.setattr(main, "market_stock_chips", fake_chips)
    monkeypatch.setattr(main, "market_stock_fund_flow", fake_flow)

    captured = {"stage1": None, "stage2": None}

    def fake_ai_strategy_candidates(*, payload):
        captured["stage1"] = payload
        return {
            "date": "2025-12-21",
            "accountId": account_id,
            "accountTitle": "Main",
            "candidates": [],
            "leader": {"symbol": "", "reason": ""},
            "model": "test-model",
        }

    monkeypatch.setattr(main, "_ai_strategy_candidates", fake_ai_strategy_candidates)

    def fake_ai_strategy_daily_markdown(*, payload):
        captured["stage2"] = payload
        return {
            "date": "2025-12-21",
            "accountId": account_id,
            "accountTitle": "Main",
            "markdown": "# Daily Strategy Report\n\n- ok\n",
            "model": "test-model",
        }

    monkeypatch.setattr(main, "_ai_strategy_daily_markdown", fake_ai_strategy_daily_markdown)

    # Generate report
    resp = client.post(
        f"/strategy/accounts/{account_id}/daily",
        json={
            "date": "2025-12-21",
            "force": False,
            "maxCandidates": 10,
            "includeAccountState": True,
            "includeTradingView": False,
            "includeIndustryFundFlow": False,
            "includeMarketSentiment": True,
            "includeLeaders": False,
            "includeStocks": False,
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["date"] == "2025-12-21"
    assert data["accountId"] == account_id
    assert "markdown" in data
    assert "Daily Strategy Report" in (data.get("markdown") or "")
    snap = data.get("inputSnapshot") or {}
    assert isinstance(snap, dict)
    assert snap.get("tradingView") == {}
    assert snap.get("industryFundFlow") == {}
    assert isinstance(snap.get("marketSentiment"), dict)
    assert (snap.get("marketSentiment") or {}).get("latest", {}).get("riskMode") == "no_new_positions"
    assert snap.get("leaderStocks") == {}
    assert snap.get("stocks") == []
    # Two-stage debug should exist in raw output.
    assert isinstance(data.get("raw"), dict)
    assert isinstance((data.get("raw") or {}).get("debug"), dict)
    assert captured["stage1"] is not None
    assert captured["stage2"] is not None
    ctx1 = (captured["stage1"] or {}).get("context") if isinstance(captured["stage1"], dict) else None
    assert isinstance(ctx1, dict)
    assert ctx1.get("leaderStocks") == {}

    # Reuse report (should not generate a new id when force=false)
    resp2 = client.post(
        f"/strategy/accounts/{account_id}/daily",
        json={"date": "2025-12-21", "force": False, "maxCandidates": 10},
    )
    assert resp2.status_code == 200
    assert resp2.json()["id"] == data["id"]

    # Get report
    resp3 = client.get(f"/strategy/accounts/{account_id}/daily?date=2025-12-21")
    assert resp3.status_code == 200
    assert resp3.json()["id"] == data["id"]

    # List reports (history)
    resp4 = client.get(f"/strategy/accounts/{account_id}/reports?days=10")
    assert resp4.status_code == 200
    hs = resp4.json()
    assert hs["accountId"] == account_id
    assert hs["days"] == 10
    items = hs.get("items") or []
    assert isinstance(items, list)
    assert items[0]["date"] == "2025-12-21"
    assert items[0]["hasMarkdown"] is True


def test_strategy_reports_prune_keeps_last_10_days(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.sqlite3"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    client = TestClient(main.app)
    acc = client.post("/broker/accounts", json={"broker": "pingan", "title": "Main"}).json()
    account_id = acc["id"]

    # Seed 12 days of reports (direct DB write).
    for i in range(12):
        d = (date(2025, 12, 1) + timedelta(days=i)).isoformat()
        main._store_strategy_report(
            report_id=f"r-{i}",
            account_id=account_id,
            date=d,
            created_at=f"{d}T00:00:00Z",
            model="test-model",
            input_snapshot={},
            output={"markdown": "# Daily Strategy Report\n\n- ok\n"},
        )

    # Prune should keep the latest 10 dates.
    main._prune_strategy_reports_keep_last_n_days(account_id=account_id, keep_days=10)
    hs = client.get(f"/strategy/accounts/{account_id}/reports?days=60").json()
    items = hs.get("items") or []
    assert isinstance(items, list)
    assert len(items) == 10
    dates = [x.get("date") for x in items if isinstance(x, dict)]
    assert "2025-12-01" not in dates
    assert "2025-12-02" not in dates
    assert "2025-12-12" in dates

