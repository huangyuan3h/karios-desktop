from fastapi.testclient import TestClient

import main
from tv.capture import CaptureResult


def test_dashboard_sync_runs_all_steps(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.sqlite3"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    client = TestClient(main.app)
    client.post("/broker/accounts", json={"broker": "pingan", "title": "Main"}).json()

    # Market providers
    monkeypatch.setattr(
        main,
        "fetch_cn_a_spot",
        lambda: [
            main.StockRow(
                symbol="CN:000001",
                market="CN",
                ticker="000001",
                name="Ping An Bank",
                currency="CNY",
                quote={"price": "10.0", "change_pct": "1.0"},
            )
        ],
    )
    monkeypatch.setattr(main, "fetch_hk_spot", lambda: [])

    # Market sentiment providers (avoid AkShare).
    monkeypatch.setattr(
        main,
        "fetch_cn_market_breadth_eod",
        lambda as_of: {
            "date": as_of.strftime("%Y-%m-%d"),
            "up_count": 500,
            "down_count": 4000,
            "flat_count": 100,
            "total_count": 4600,
            "up_down_ratio": 0.125,
            "raw": {},
        },
    )
    monkeypatch.setattr(
        main,
        "fetch_cn_yesterday_limitup_premium",
        lambda as_of: {"date": as_of.strftime("%Y-%m-%d"), "premium": -0.5, "count": 50, "raw": {}},
    )
    monkeypatch.setattr(
        main,
        "fetch_cn_failed_limitup_rate",
        lambda as_of: {"date": as_of.strftime("%Y-%m-%d"), "failed_rate": 35.0, "ever_count": 100, "close_count": 65, "raw": {}},
    )

    # Industry fund flow providers
    monkeypatch.setattr(
        main,
        "fetch_cn_industry_fund_flow_eod",
        lambda as_of: [
            {
                "date": as_of.strftime("%Y-%m-%d"),
                "industry_code": "abc",
                "industry_name": "Bank",
                "net_inflow": 123.0,
                "raw": {},
            }
        ],
    )
    monkeypatch.setattr(main, "fetch_cn_industry_fund_flow_hist", lambda name, industry_code=None, days=10: [])

    # TradingView capture: avoid real CDP/Playwright.
    monkeypatch.setattr(main, "_cdp_version", lambda host, port: {"Browser": "Chrome"})
    monkeypatch.setattr(
        main,
        "capture_screener_over_cdp_sync",
        lambda *, cdp_url, url: CaptureResult(
            url=str(url),
            captured_at="2025-12-21T00:00:00Z",
            screen_title="Test Screener",
            filters=["TestFilter"],
            headers=["Symbol", "Price"],
            rows=[{"Symbol": "000001", "Price": "10"}],
        ),
    )

    # Mainline generation: avoid AkShare/AI calls.
    monkeypatch.setattr(
        main,
        "_build_mainline_snapshot",
        lambda **_k: {
            "tradeDate": "2025-12-21",
            "asOfTs": "2025-12-21T00:00:00Z",
            "accountId": "aid",
            "universeVersion": "v0",
            "riskMode": "caution",
            "selected": {"kind": "industry", "name": "Bank", "compositeScore": 80},
            "themesTopK": [{"kind": "industry", "name": "Bank", "compositeScore": 80, "structureScore": 80, "logicScore": 80}],
            "debug": {},
        },
    )

    resp = client.post("/dashboard/sync", json={"force": True})
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] in (True, False)
    names = {s["name"] for s in data["steps"]}
    assert {"market", "industryFundFlow", "marketSentiment", "screeners", "mainline"} <= names
    assert data["screener"]["enabledCount"] >= 1
    assert isinstance(data["screener"]["items"], list)


def test_dashboard_summary_shape(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.sqlite3"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    # Create account and seed a minimal consolidated state.
    client = TestClient(main.app)
    acc = client.post(
        "/broker/accounts",
        json={"broker": "pingan", "title": "Main", "accountMasked": "3260****7775"},
    ).json()
    account_id = acc["id"]
    main._upsert_account_state(
        account_id=account_id,
        broker="pingan",
        updated_at="2025-12-21T00:00:00Z",
        overview={"totalAssets": "1000000", "cashAvailable": "500000"},
        positions=[{"ticker": "000001", "name": "Ping An Bank", "qtyHeld": "1000", "price": "10.0"}],
        conditional_orders=[],
        trades=[],
    )

    # Seed one leader + minimal market stock row so forced bars/chips/flow can run.
    with main._connect() as conn:
        ts = "2025-12-21T00:00:00Z"
        conn.execute(
            """
            INSERT OR REPLACE INTO market_stocks(symbol, market, ticker, name, currency, updated_at)
            VALUES(?, ?, ?, ?, ?, ?)
            """,
            ("CN:000001", "CN", "000001", "Ping An Bank", "CNY", ts),
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO leader_stocks(
              id, date, symbol, market, ticker, name, entry_price, score, reason, source_signals_json, risk_points_json, created_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "leader-1",
                "2025-12-21",
                "CN:000001",
                "CN",
                "000001",
                "Ping An Bank",
                10.0,
                90.0,
                "Test leader",
                "{}",
                "[]",
                ts,
            ),
        )
        conn.commit()

    # Force-refresh dependencies for leaders (avoid AkShare).
    monkeypatch.setattr(
        main,
        "fetch_cn_a_daily_bars",
        lambda ticker, days=60: [
            main.BarRow(
                date="2025-12-21",
                open="10",
                high="11",
                low="9",
                close="10",
                volume="100",
                amount="1000",
            )
        ],
    )
    monkeypatch.setattr(main, "fetch_cn_a_chip_summary", lambda ticker, days=30: [])
    monkeypatch.setattr(main, "fetch_cn_a_fund_flow", lambda ticker, days=30: [])

    # Seed one sentiment day.
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

    resp = client.get(f"/dashboard/summary?accountId={account_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["selectedAccountId"] == account_id
    assert isinstance(data["accounts"], list)
    assert isinstance(data.get("marketStatus"), dict)
    assert isinstance(data.get("industryFundFlow"), dict)
    assert isinstance(data.get("marketSentiment"), dict)
    assert isinstance(data.get("leaders"), dict)
    assert isinstance(data.get("screeners"), list)


