from fastapi.testclient import TestClient

import main


def _seed_tv_snapshot() -> None:
    with main._connect() as conn:
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
                "snap-leader-1",
                "falcon",
                "2025-12-21T00:00:00Z",
                2,
                '["Symbol","Price"]',
                main.json.dumps(payload, ensure_ascii=False),
            ),
        )
        conn.commit()


def test_leader_daily_generation_and_entry_price(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.sqlite3"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    _seed_tv_snapshot()

    # Avoid AkShare calls.
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

    monkeypatch.setattr(main, "market_stock_chips", fake_chips)
    monkeypatch.setattr(main, "market_stock_fund_flow", fake_flow)

    # Bars should include selection date close for entryPrice.
    def fake_bars(symbol: str, days: int = 60, force: bool = False):
        return main.MarketBarsResponse(
            symbol=symbol,
            market="CN",
            ticker=symbol.split(":")[1],
            name="Test",
            currency="CNY",
            bars=[
                {
                    "date": "2025-12-20",
                    "open": "10",
                    "high": "11",
                    "low": "9",
                    "close": "10",
                    "volume": "100",
                    "amount": "1000",
                },
                {
                    "date": "2025-12-21",
                    "open": "10",
                    "high": "12",
                    "low": "10",
                    "close": "11",
                    "volume": "120",
                    "amount": "1200",
                },
            ],
        )

    monkeypatch.setattr(main, "market_stock_bars", fake_bars)

    captured: dict[str, object] = {}

    def fake_ai_leader_daily(*, payload):
        captured["payload"] = payload
        return {
            "date": payload["date"],
            "leaders": [
                {
                    "symbol": "CN:300502",
                    "market": "CN",
                    "ticker": "300502",
                    "name": "Xin Yi Sheng",
                    "score": 92,
                    "reason": "行业资金主线+强趋势，具备龙头属性。",
                    "whyBullets": ["资金主线", "趋势强", "相对强势"],
                    "expectedDurationDays": 5,
                    "buyZone": {"low": 10.8, "high": 11.2, "note": "pullback zone"},
                    "triggers": [{"kind": "breakout", "condition": "break above", "value": 11.3}],
                    "invalidation": "close below 10.5",
                    "targetPrice": {"primary": 12.5, "stretch": 13.2},
                    "probability": 4,
                    "risks": ["High volatility", "Gap risk"],
                    "sourceSignals": {"industries": ["Test"], "screeners": ["falcon"]},
                    "riskPoints": ["High volatility", "Gap risk"],
                },
                {
                    "symbol": "CN:300308",
                    "market": "CN",
                    "ticker": "300308",
                    "name": "Zhong Ji Xuchuang",
                    "score": 88,
                    "reason": "相对强势，资金延续性更好。",
                    "sourceSignals": {"industries": ["Test"], "screeners": ["falcon"]},
                    "riskPoints": ["False breakout"],
                },
            ],
            "model": "test-model",
        }

    monkeypatch.setattr(main, "_ai_leader_daily", fake_ai_leader_daily)

    client = TestClient(main.app)

    resp = client.post("/leader/daily", json={"date": "2025-12-21", "force": True})
    assert resp.status_code == 200
    data = resp.json()
    assert data["date"] == "2025-12-21"
    assert isinstance(data["leaders"], list)
    assert 1 <= len(data["leaders"]) <= 2
    assert "debug" in data
    assert captured.get("payload") is not None

    l0 = data["leaders"][0]
    assert l0["entryPrice"] == 11.0
    assert l0["symbol"].startswith("CN:")
    assert l0["whyBullets"] == ["资金主线", "趋势强", "相对强势"]
    assert l0["expectedDurationDays"] == 5
    assert isinstance(l0["buyZone"], dict)
    assert isinstance(l0["triggers"], list)
    assert l0["invalidation"]
    assert isinstance(l0["targetPrice"], dict)
    assert l0["probability"] == 4
    assert isinstance(l0["risks"], list)

    resp2 = client.get("/leader?days=10")
    assert resp2.status_code == 200
    lst = resp2.json()
    assert "leaders" in lst
    assert any(x["date"] == "2025-12-21" for x in lst["leaders"])
    l0b = next(x for x in lst["leaders"] if x["date"] == "2025-12-21" and x["ticker"] == "300502")
    assert l0b["expectedDurationDays"] == 5
    assert isinstance(l0b["buyZone"], dict)
    assert isinstance(l0b["targetPrice"], dict)


def test_leader_retention_keeps_last_10_dates(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.sqlite3"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    _seed_tv_snapshot()

    monkeypatch.setattr(
        main,
        "market_stock_chips",
        lambda symbol, days=30: main.MarketChipsResponse(
            symbol=symbol,
            market="CN",
            ticker=symbol.split(":")[1],
            name="Test",
            currency="CNY",
            items=[],
        ),
    )
    monkeypatch.setattr(
        main,
        "market_stock_fund_flow",
        lambda symbol, days=30: main.MarketFundFlowResponse(
            symbol=symbol,
            market="CN",
            ticker=symbol.split(":")[1],
            name="Test",
            currency="CNY",
            items=[],
        ),
    )

    def fake_bars(symbol: str, days: int = 180):
        bars = []
        for i in range(1, 13):
            d = f"2025-12-{i:02d}"
            close = 10 + i
            bars.append(
                {
                    "date": d,
                    "open": str(close - 1),
                    "high": str(close + 1),
                    "low": str(close - 2),
                    "close": str(close),
                    "volume": "100",
                    "amount": "1000",
                }
            )
        return main.MarketBarsResponse(
            symbol=symbol,
            market="CN",
            ticker=symbol.split(":")[1],
            name="Test",
            currency="CNY",
            bars=bars,
        )

    monkeypatch.setattr(main, "market_stock_bars", fake_bars)

    monkeypatch.setattr(
        main,
        "_ai_leader_daily",
        lambda *, payload: {
            "date": payload["date"],
            "leaders": [
                {
                    "symbol": "CN:300502",
                    "market": "CN",
                    "ticker": "300502",
                    "name": "Xin Yi Sheng",
                    "score": 90,
                    "reason": "Still leader",
                    "sourceSignals": {"screeners": ["falcon"]},
                    "riskPoints": [],
                }
            ],
            "model": "test-model",
        },
    )

    client = TestClient(main.app)

    for i in range(1, 13):
        d = f"2025-12-{i:02d}"
        resp = client.post("/leader/daily", json={"date": d, "force": True})
        assert resp.status_code == 200

    resp2 = client.get("/leader?days=30")
    assert resp2.status_code == 200
    data = resp2.json()
    dates = sorted({x["date"] for x in data["leaders"]})
    assert len(dates) == 10
    assert dates[0] == "2025-12-03"
    assert dates[-1] == "2025-12-12"


