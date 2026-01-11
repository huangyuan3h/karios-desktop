from datetime import date, timedelta

from fastapi.testclient import TestClient

import main


def _seed_tv_snapshot(*, db_path, screener_id: str, rows: list[dict[str, str]]) -> None:
    # Ensure default screeners exist.
    main._seed_default_tv_screeners()
    with main._connect() as conn:
        payload = {
            "screenTitle": "RankTest",
            "filters": ["TestFilter"],
            "url": "https://www.tradingview.com/screener/falcon/",
            "headers": ["Symbol", "Price", "Sector"],
            "rows": rows,
        }
        conn.execute(
            """
            INSERT INTO tv_screener_snapshots(id, screener_id, captured_at, row_count, headers_json, rows_json)
            VALUES(?, ?, ?, ?, ?, ?)
            """,
            (
                "rank-snap-1",
                screener_id,
                "2026-01-07T00:00:00Z",
                len(rows),
                '["Symbol","Price","Sector"]',
                main.json.dumps(payload, ensure_ascii=False),
            ),
        )
        conn.commit()


def _seed_cn_stock_basic(symbol: str, ticker: str, name: str) -> None:
    main._ensure_market_stock_basic(symbol=symbol, market="CN", ticker=ticker, name=name, currency="CNY")


def _seed_bars(symbol: str, *, start: str, n: int, close0: float, step: float, amount: float, vol0: float) -> None:
    # Insert N daily bars with uptrend and increasing volume.
    d0 = date.fromisoformat(start)
    with main._connect() as conn:
        ts = "2026-01-07T00:00:00Z"
        for i in range(n):
            d = (d0 + timedelta(days=i)).isoformat()
            close = close0 + step * i
            high = close * 1.01
            low = close * 0.99
            vol = vol0 + i * (vol0 * 0.05)
            conn.execute(
                """
                INSERT INTO market_bars(symbol, date, open, high, low, close, volume, amount, updated_at)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    symbol,
                    d,
                    str(close),
                    str(high),
                    str(low),
                    str(close),
                    str(vol),
                    str(amount),
                    ts,
                ),
            )
        conn.commit()


def _seed_flow(symbol: str, *, d: str, main_ratio: float) -> None:
    with main._connect() as conn:
        ts = "2026-01-07T00:00:00Z"
        raw = {
            "date": d,
            "mainNetRatio": main_ratio,
            "superNetRatio": 0.8,
            "largeNetRatio": 0.6,
        }
        conn.execute(
            """
            INSERT INTO market_fund_flow(
              symbol, date, close, change_pct,
              main_net_amount, main_net_ratio,
              super_net_amount, super_net_ratio,
              large_net_amount, large_net_ratio,
              medium_net_amount, medium_net_ratio,
              small_net_amount, small_net_ratio,
              updated_at, raw_json
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                symbol,
                d,
                "",
                "",
                "",
                str(main_ratio),
                "",
                "0.8",
                "",
                "0.6",
                "",
                "",
                "",
                "",
                ts,
                main.json.dumps(raw, ensure_ascii=False),
            ),
        )
        conn.commit()


def _seed_chips(symbol: str, *, d: str, profit_ratio: float, avg_cost: float) -> None:
    with main._connect() as conn:
        ts = "2026-01-07T00:00:00Z"
        raw = {
            "date": d,
            "profitRatio": profit_ratio,
            "avgCost": avg_cost,
            "cost70Conc": 0.10,
            "cost70Low": avg_cost * 0.95,
            "cost70High": avg_cost * 1.05,
            "cost90Low": avg_cost * 0.90,
            "cost90High": avg_cost * 1.10,
        }
        conn.execute(
            """
            INSERT INTO market_chips(
              symbol, date,
              profit_ratio, avg_cost, cost90_low, cost90_high, cost90_conc,
              cost70_low, cost70_high, cost70_conc,
              updated_at, raw_json
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                symbol,
                d,
                str(profit_ratio),
                str(avg_cost),
                str(avg_cost * 0.90),
                str(avg_cost * 1.10),
                "",
                str(avg_cost * 0.95),
                str(avg_cost * 1.05),
                "0.10",
                ts,
                main.json.dumps(raw, ensure_ascii=False),
            ),
        )
        conn.commit()


def test_rank_next2d_generate_and_read(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.sqlite3"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    client = TestClient(main.app)
    acc = client.post(
        "/broker/accounts",
        json={"broker": "pingan", "title": "Main", "accountMasked": "3260****7775"},
    ).json()
    account_id = acc["id"]

    # Seed consolidated holdings: include a holding not in TV pool.
    main._upsert_account_state(
        account_id=account_id,
        broker="pingan",
        updated_at="2026-01-07T00:00:00Z",
        overview={"totalAssets": "1000000", "cashAvailable": "500000"},
        positions=[{"ticker": "000003", "name": "HoldingOnly", "qtyHeld": "100", "price": "10.0"}],
        conditional_orders=[],
        trades=[],
    )

    # Seed sentiment as no_new_positions to verify riskMode is carried and penalty applied.
    main._upsert_cn_sentiment_daily(
        date="2026-01-07",
        as_of_date="2026-01-07",
        up=100,
        down=1000,
        flat=50,
        up_down_ratio=0.1,
        premium=-0.5,
        failed_rate=40.0,
        risk_mode="no_new_positions",
        rules=["premium<0 && failedLimitUpRate>30 => no_new_positions"],
        updated_at="2026-01-07T00:00:00Z",
        raw={},
    )

    # Seed TV pool: one strong, one weak.
    _seed_tv_snapshot(
        db_path=db_path,
        screener_id="falcon",
        rows=[
            {"Symbol": "000001\nStrongOne\nD", "Price": "10 CNY", "Sector": "Bank"},
            {"Symbol": "000002\nWeakOne\nD", "Price": "10 CNY", "Sector": "Bank"},
        ],
    )

    # Seed market basics and bars/flow/chips.
    _seed_cn_stock_basic("CN:000001", "000001", "StrongOne")
    _seed_cn_stock_basic("CN:000002", "000002", "WeakOne")
    _seed_cn_stock_basic("CN:000003", "000003", "HoldingOnly")

    # Avoid AkShare and ai-service calls in tests.
    monkeypatch.setattr(main, "fetch_cn_a_spot", lambda: [])
    monkeypatch.setattr(
        main,
        "_ai_quant_rank_explain",
        lambda *, payload: {
            "asOfTs": payload.get("asOfTs", ""),
            "asOfDate": payload.get("asOfDate", ""),
            "items": [{"symbol": c.get("symbol", ""), "llmScoreAdj": 0, "whyBullets": [{"text": "ok", "evidenceRefs": ["breakdown"]}]} for c in (payload.get("candidates") or []) if isinstance(c, dict)],
            "model": "test-model",
        },
    )

    # StrongOne: uptrend + breakout-ish + enough liquidity.
    _seed_bars("CN:000001", start="2025-12-10", n=25, close0=10.0, step=0.2, amount=2e8, vol0=1000)
    _seed_flow("CN:000001", d="2026-01-07", main_ratio=3.0)
    _seed_chips("CN:000001", d="2026-01-07", profit_ratio=0.7, avg_cost=12.0)

    # WeakOne: low liquidity amount -> filtered out (not holding).
    _seed_bars("CN:000002", start="2025-12-10", n=25, close0=10.0, step=0.05, amount=2e7, vol0=800)
    _seed_flow("CN:000002", d="2026-01-07", main_ratio=-2.0)
    _seed_chips("CN:000002", d="2026-01-07", profit_ratio=0.3, avg_cost=10.5)

    # HoldingOnly: minimal bars (still included because holding bypasses momentum filter).
    _seed_bars("CN:000003", start="2025-12-20", n=10, close0=10.0, step=0.0, amount=0.0, vol0=0.0)

    # Generate
    resp = client.post(
        "/rank/cn/next2d/generate",
        json={"accountId": account_id, "asOfDate": "2026-01-07", "force": True, "limit": 30, "includeHoldings": True},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["asOfDate"] == "2026-01-07"
    assert data["accountId"] == account_id
    assert data["riskMode"] == "no_new_positions"
    tickers = [it["ticker"] for it in data["items"]]
    assert "000001" in tickers
    assert "000003" in tickers
    assert "000002" not in tickers
    # StrongOne should rank above holding-only.
    assert tickers.index("000001") < tickers.index("000003")
    # Ensure risk penalty is present in breakdown.
    row0 = next((x for x in data["items"] if x["ticker"] == "000001"), None)
    assert isinstance(row0, dict)
    assert isinstance(row0.get("breakdown"), dict)
    assert float(row0["breakdown"].get("riskPenalty") or 0.0) < 0.0
    # Bootstrap behavior: without enough calibration data, score should fall back to rawScore.
    assert float(row0.get("rawScore") or 0.0) == float(row0.get("score") or 0.0)

    # Read cached snapshot
    resp2 = client.get(f"/rank/cn/next2d?accountId={account_id}&asOfDate=2026-01-07&limit=30")
    assert resp2.status_code == 200
    data2 = resp2.json()
    assert data2["asOfDate"] == "2026-01-07"
    assert len(data2["items"]) >= 2


