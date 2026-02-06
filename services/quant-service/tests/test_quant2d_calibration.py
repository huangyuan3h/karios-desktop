from datetime import date, timedelta

from fastapi.testclient import TestClient

import main


def _seed_bar(symbol: str, d: str, *, close: float, low: float) -> None:
    with main._connect() as conn:
        conn.execute(
            """
            INSERT INTO market_bars(symbol, date, open, high, low, close, volume, amount, updated_at)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                symbol,
                d,
                str(close),
                str(close),
                str(low),
                str(close),
                "100",
                "100000000",
                f"{d}T00:00:00Z",
            ),
        )
        conn.commit()


def test_quant2d_outcome_label_and_calibration(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.sqlite3"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    client = TestClient(main.app)
    acc = client.post("/broker/accounts", json={"broker": "pingan", "title": "Main"}).json()
    account_id = acc["id"]

    # Event date is a Thursday so next 2 trading days are Fri + Mon.
    as_of_date = "2026-01-01"
    as_of_ts = "2026-01-01T03:00:00Z"
    sym = "CN:000001"
    main._ensure_market_stock_basic(symbol=sym, market="CN", ticker="000001", name="Alpha", currency="CNY")

    # Seed next 2 trading days bars: close up, with a small pullback low.
    _seed_bar(sym, "2026-01-02", close=10.5, low=9.7)
    _seed_bar(sym, "2026-01-05", close=10.2, low=9.8)

    main._upsert_quant_2d_rank_events(
        account_id=account_id,
        as_of_ts=as_of_ts,
        as_of_date=as_of_date,
        rows=[
            {
                "symbol": sym,
                "ticker": "000001",
                "name": "Alpha",
                "buyPrice": 10.0,
                "buyPriceSrc": "spot",
                "rawScore": 90.0,
                "evidence": {"breakdown": {"trend": 0.9}},
            }
        ],
    )

    meta = main._label_quant_2d_outcomes_best_effort(account_id=account_id, as_of_date=as_of_date, limit=50)
    assert meta["labeled"] == 1

    with main._connect() as conn:
        row = conn.execute(
            "SELECT ret2d_avg_pct, dd2d_pct, win FROM quant_2d_outcomes WHERE account_id = ?",
            (account_id,),
        ).fetchone()
    assert row is not None
    ret2d = float(row[0] or 0.0)
    dd2d = float(row[1] or 0.0)
    win = int(row[2] or 0)
    # avg(close_t1, close_t2)/buy - 1 => avg(10.5, 10.2)/10 - 1 = 3.5%
    assert abs(ret2d - 3.5) < 1e-6
    # low_min/buy - 1 => min(9.7, 9.8)/10 - 1 = -3.0%
    assert abs(dd2d - (-3.0)) < 1e-6
    assert win == 1

    cal = main._build_quant_2d_calibration(account_id=account_id, buckets=10, lookback_days=30)
    assert cal["n"] >= 1
    assert isinstance(cal.get("items"), list)


def test_quant2d_calibration_uses_latest_date_window(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.sqlite3"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    client = TestClient(main.app)
    acc = client.post("/broker/accounts", json={"broker": "pingan", "title": "Main"}).json()
    account_id = acc["id"]

    with main._connect() as conn:
        conn.execute(
            """
            INSERT INTO quant_2d_rank_events(
              id, account_id, as_of_ts, as_of_date, symbol, ticker, name, buy_price, buy_price_src, raw_score,
              evidence_json, created_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "evt-1",
                account_id,
                "2020-01-01T00:00:00Z",
                "2020-01-01",
                "CN:000001",
                "000001",
                "Alpha",
                10.0,
                "spot",
                80.0,
                main.json.dumps({"breakdown": {"trend": 0.8}}, ensure_ascii=False),
                "2020-01-01T00:00:00Z",
            ),
        )
        conn.execute(
            """
            INSERT INTO quant_2d_outcomes(
              event_id, account_id, as_of_ts, as_of_date, symbol, buy_price, t1_date, t2_date, close_t1, close_t2,
              low_min, ret2d_avg_pct, dd2d_pct, win, labeled_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "evt-1",
                account_id,
                "2020-01-01T00:00:00Z",
                "2020-01-01",
                "CN:000001",
                10.0,
                "2020-01-02",
                "2020-01-03",
                10.1,
                10.2,
                9.9,
                1.5,
                -1.0,
                1,
                "2020-01-03T00:00:00Z",
            ),
        )
        conn.execute(
            """
            INSERT INTO quant_2d_rank_events(
              id, account_id, as_of_ts, as_of_date, symbol, ticker, name, buy_price, buy_price_src, raw_score,
              evidence_json, created_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "evt-2",
                account_id,
                "2020-01-15T00:00:00Z",
                "2020-01-15",
                "CN:000002",
                "000002",
                "Beta",
                12.0,
                "spot",
                90.0,
                main.json.dumps({"breakdown": {"trend": 0.9}}, ensure_ascii=False),
                "2020-01-15T00:00:00Z",
            ),
        )
        conn.execute(
            """
            INSERT INTO quant_2d_outcomes(
              event_id, account_id, as_of_ts, as_of_date, symbol, buy_price, t1_date, t2_date, close_t1, close_t2,
              low_min, ret2d_avg_pct, dd2d_pct, win, labeled_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "evt-2",
                account_id,
                "2020-01-15T00:00:00Z",
                "2020-01-15",
                "CN:000002",
                12.0,
                "2020-01-16",
                "2020-01-17",
                12.4,
                12.2,
                11.7,
                2.0,
                -2.5,
                1,
                "2020-01-17T00:00:00Z",
            ),
        )
        conn.commit()

    cal = main._build_quant_2d_calibration(account_id=account_id, buckets=10, lookback_days=30)
    assert cal["n"] == 2
    assert isinstance(cal.get("items"), list)


def test_quant2d_llm_adjust_requires_valid_evidence_refs(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.sqlite3"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    client = TestClient(main.app)
    acc = client.post("/broker/accounts", json={"broker": "pingan", "title": "Main"}).json()
    account_id = acc["id"]

    # Seed TV snapshot and minimal market bars for one symbol so rank can run.
    main._seed_default_tv_screeners()
    with main._connect() as conn:
        payload = {
            "screenTitle": "RankTest",
            "filters": ["TestFilter"],
            "url": "https://www.tradingview.com/screener/falcon/",
            "headers": ["Symbol", "Price", "Sector"],
            "rows": [{"Symbol": "000001\nAlpha\nD", "Price": "10 CNY", "Sector": "Bank"}],
        }
        conn.execute(
            """
            INSERT INTO tv_screener_snapshots(id, screener_id, captured_at, row_count, headers_json, rows_json)
            VALUES(?, ?, ?, ?, ?, ?)
            """,
            ("rank-snap-llm-1", "falcon", "2026-01-07T00:00:00Z", 1, '["Symbol","Price","Sector"]', main.json.dumps(payload, ensure_ascii=False)),
        )
        conn.commit()

    main._ensure_market_stock_basic(symbol="CN:000001", market="CN", ticker="000001", name="Alpha", currency="CNY")

    # 60 bars so it passes filters.
    d0 = date.fromisoformat("2025-12-01")
    with main._connect() as conn:
        for i in range(60):
            d = (d0 + timedelta(days=i)).isoformat()
            close = 10.0 + 0.05 * i
            high = close * 1.01
            low = close * 0.99
            conn.execute(
                """
                INSERT INTO market_bars(symbol, date, open, high, low, close, volume, amount, updated_at)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "CN:000001",
                    d,
                    str(close),
                    str(high),
                    str(low),
                    str(close),
                    str(1000 + i * 10),
                    "200000000",
                    "2026-01-07T00:00:00Z",
                ),
            )
        conn.commit()

    # Disable AkShare spot.
    monkeypatch.setattr(main, "fetch_cn_a_spot", lambda: [])

    # LLM returns +5 adj but with invalid evidenceRefs => should NOT be applied.
    def fake_llm(*, payload):
        c = (payload.get("candidates") or [])[0]
        return {
            "asOfTs": payload.get("asOfTs", ""),
            "asOfDate": payload.get("asOfDate", ""),
            "items": [
                {
                    "symbol": c.get("symbol", ""),
                    "llmScoreAdj": 5,
                    "whyBullets": [{"text": "invalid ref", "evidenceRefs": ["does.not.exist"]}],
                }
            ],
            "model": "test-model",
        }

    monkeypatch.setattr(main, "_ai_quant_rank_explain", fake_llm)

    resp = client.post(
        "/rank/cn/next2d/generate",
        json={"accountId": account_id, "asOfDate": "2026-01-07", "force": True, "limit": 30, "includeHoldings": True},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["items"]
    top1 = data["items"][0]
    # LLM adjustment should not be applied; whyBullets should not be replaced by invalid ref bullet.
    assert "invalid ref" not in " ".join(top1.get("whyBullets") or [])

