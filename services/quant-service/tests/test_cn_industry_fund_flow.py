from fastapi.testclient import TestClient

import main


def test_cn_industry_fund_flow_sync_and_query(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.sqlite3"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    as_of = "2025-12-26"

    # Avoid AkShare/network by patching provider-facing functions in main.
    def fake_eod(_d):
        return [
            {
                "date": as_of,
                "industry_code": "BK_A",
                "industry_name": "Power",
                "net_inflow": 5_000_000_000.0,
                "raw": {"x": 1},
            },
            {
                "date": as_of,
                "industry_code": "BK_B",
                "industry_name": "Semiconductor",
                "net_inflow": 1_000_000_000.0,
                "raw": {"x": 2},
            },
        ]

    def fake_hist(name: str, *, days: int = 10):
        _ = days
        if name == "Power":
            return [
                {"date": "2025-12-25", "net_inflow": 1.0, "raw": {}},
                {"date": "2025-12-26", "net_inflow": 2.0, "raw": {}},
            ]
        return [{"date": "2025-12-26", "net_inflow": 3.0, "raw": {}}]

    monkeypatch.setattr(main, "fetch_cn_industry_fund_flow_eod", fake_eod)
    monkeypatch.setattr(main, "fetch_cn_industry_fund_flow_hist", fake_hist)

    client = TestClient(main.app)

    # First sync writes snapshot rows + hist for Top1.
    resp = client.post(
        "/market/cn/industry-fund-flow/sync",
        json={"date": as_of, "days": 10, "topN": 1, "force": True},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["asOfDate"] == as_of
    assert data["rowsUpserted"] == 2
    # Hist backfill should NOT overwrite asOfDate snapshot values.
    assert data["histRowsUpserted"] == 1

    # Query should return Power first by asOfDate netInflow.
    resp = client.get(f"/market/cn/industry-fund-flow?days=10&topN=10&asOfDate={as_of}")
    assert resp.status_code == 200
    q = resp.json()
    assert q["asOfDate"] == as_of
    assert isinstance(q["dates"], list)
    assert q["top"][0]["industryName"] == "Power"
    assert q["top"][0]["netInflow"] == 5_000_000_000.0
    assert len(q["top"][0]["series10d"]) >= 1

    # Second sync without force should skip due to cache.
    resp2 = client.post(
        "/market/cn/industry-fund-flow/sync",
        json={"date": as_of, "days": 10, "topN": 1, "force": False},
    )
    assert resp2.status_code == 200
    assert resp2.json()["rowsUpserted"] == 0


def test_strategy_injects_industry_fund_flow(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.sqlite3"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    client = TestClient(main.app)
    acc = client.post(
        "/broker/accounts",
        json={"broker": "pingan", "title": "Main", "accountMasked": "3260****7775"},
    ).json()
    account_id = acc["id"]

    # Minimal TV snapshot to avoid empty pool.
    with main._connect() as conn:
        main._seed_default_tv_screeners()
        payload = {
            "screenTitle": "Falcon",
            "filters": ["TestFilter"],
            "url": "https://www.tradingview.com/screener/falcon/",
            "headers": ["Symbol", "Price"],
            "rows": [{"Symbol": "300502\nXin Yi Sheng\nD", "Price": "434.32 CNY"}],
        }
        conn.execute(
            """
            INSERT INTO tv_screener_snapshots(id, screener_id, captured_at, row_count, headers_json, rows_json)
            VALUES(?, ?, ?, ?, ?, ?)
            """,
            (
                "snap-ff",
                "falcon",
                "2025-12-26T00:00:00Z",
                1,
                '["Symbol","Price"]',
                main.json.dumps(payload, ensure_ascii=False),
            ),
        )
        conn.commit()

    # Avoid market deep data calls.
    monkeypatch.setattr(
        main,
        "market_stock_bars",
        lambda symbol, days=60: main.MarketBarsResponse(
            symbol=symbol,
            market="CN",
            ticker=symbol.split(":")[1],
            name="Test",
            currency="CNY",
            bars=[],
        ),
    )
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

    # Patch industry flow query to return deterministic context (so we don't need DB writes).
    flow = main.MarketCnIndustryFundFlowResponse(
        asOfDate="2025-12-26",
        days=10,
        topN=10,
        dates=["2025-12-26"],
        top=[
            main.IndustryFundFlowRow(
                industryCode="BK_A",
                industryName="Power",
                netInflow=123.0,
                sum10d=456.0,
                series10d=[main.IndustryFundFlowPoint(date="2025-12-26", netInflow=123.0)],
            )
        ],
    )
    monkeypatch.setattr(main, "market_cn_industry_fund_flow", lambda days=10, topN=10, asOfDate=None: flow)

    captured = {"stage1": None, "stage2": None}

    # Stage 1 candidates call (avoid ai-service).
    monkeypatch.setattr(
        main,
        "_ai_strategy_candidates",
        lambda *, payload: (
            captured.__setitem__("stage1", payload)
            or {
            "date": "2025-12-26",
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
                    "why": "test",
                    "scoreBreakdown": {"trend": 30, "flow": 25, "structure": 20, "risk": 13},
                }
            ],
            "leader": {"symbol": "CN:300502", "reason": "test"},
            "model": "test-model",
        }
        ),
    )

    # Avoid AI call.
    monkeypatch.setattr(
        main,
        "_ai_strategy_daily_markdown",
        lambda *, payload: (
            captured.__setitem__("stage2", payload)
            or {
            "date": "2025-12-26",
            "accountId": account_id,
            "accountTitle": "Main",
            "markdown": "# Main 日度交易报告（2025-12-26）\n\n## 1）资金流向板块\n- Power\n",
            "model": "test-model",
        }
        ),
    )

    resp = client.post(
        f"/strategy/accounts/{account_id}/daily",
        json={"date": "2025-12-26", "force": True, "maxCandidates": 5},
    )
    assert resp.status_code == 200
    rep = resp.json()
    ctx = rep.get("inputSnapshot") or {}
    assert "industryFundFlow" in ctx
    assert (ctx["industryFundFlow"].get("top") or [])[0]["industryName"] == "Power"
    assert captured["stage1"] is not None
    assert captured["stage2"] is not None


