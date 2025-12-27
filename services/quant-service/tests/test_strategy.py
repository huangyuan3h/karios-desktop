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

    # Avoid AkShare and AI calls in tests.
    def fake_bars(symbol: str, days: int = 60):
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

    def fake_chips(symbol: str, days: int = 30):
        return main.MarketChipsResponse(
            symbol=symbol,
            market="CN",
            ticker=symbol.split(":")[1],
            name="Test",
            currency="CNY",
            items=[],
        )

    def fake_flow(symbol: str, days: int = 30):
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

    def fake_ai_strategy_daily(*, payload):
        _ = payload
        return {
            "date": "2025-12-21",
            "accountId": account_id,
            "accountTitle": "Main",
            "candidates": [
                {
                    "symbol": "CN:300502",
                    "market": "CN",
                    "ticker": "300502",
                    "name": "Xin Yi Sheng",
                    "score": 88,
                    "rank": 1,
                    "why": "Strong momentum",
                }
            ],
            "leader": {"symbol": "CN:300502", "reason": "Leader by volume and trend"},
            "recommendations": [
                {
                    "symbol": "CN:300502",
                    "ticker": "300502",
                    "name": "Xin Yi Sheng",
                    "thesis": "Breakout continuation",
                    "levels": {"support": ["420"], "resistance": ["445"], "invalidations": ["< 410"]},
                    "orders": [
                        {"kind": "breakout_buy", "side": "buy", "trigger": "price >= 445", "qty": "10% equity", "timeInForce": "day"}
                    ],
                    "positionSizing": "Max 20% for leader",
                    "riskNotes": ["Hard stop below 410"],
                }
            ],
            "riskNotes": ["Do not exceed 3 positions"],
            "model": "test-model",
        }

    monkeypatch.setattr(main, "_ai_strategy_daily", fake_ai_strategy_daily)

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
            "includeStocks": False,
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["date"] == "2025-12-21"
    assert data["accountId"] == account_id
    assert data["candidates"][0]["ticker"] == "300502"
    assert data["leader"]["symbol"] == "CN:300502"
    snap = data.get("inputSnapshot") or {}
    assert isinstance(snap, dict)
    assert snap.get("tradingView") == {}
    assert snap.get("industryFundFlow") == {}
    assert snap.get("stocks") == []

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


