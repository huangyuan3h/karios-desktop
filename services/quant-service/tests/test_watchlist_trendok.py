from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient

import main


def _seed_market_stock(symbol: str, market: str, ticker: str, name: str, currency: str = "CNY") -> None:
    # Uses main._connect() to ensure DB schema is initialized.
    main._ensure_market_stock_basic(symbol=symbol, market=market, ticker=ticker, name=name, currency=currency)


def _seed_market_bars(symbol: str, start_date: str, closes: list[float], vols: list[float]) -> None:
    assert len(closes) == len(vols)
    start = datetime.fromisoformat(start_date).replace(tzinfo=UTC)
    with main._connect() as conn:
        for i, (c, v) in enumerate(zip(closes, vols)):
            d = (start + timedelta(days=i)).date().isoformat()
            conn.execute(
                """
                INSERT INTO market_bars(symbol, date, open, high, low, close, volume, amount, updated_at)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, date) DO UPDATE SET
                  close=excluded.close,
                  volume=excluded.volume,
                  updated_at=excluded.updated_at
                """,
                (
                    symbol,
                    d,
                    str(c),
                    str(c),
                    str(c),
                    str(c),
                    str(v),
                    str(c * v),
                    "2026-01-10T00:00:00Z",
                ),
            )
        conn.commit()


def test_watchlist_trendok_insufficient_data(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.sqlite3"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    client = TestClient(main.app)
    _seed_market_stock("CN:000001", "CN", "000001", "Alpha")
    _seed_market_bars("CN:000001", "2026-01-01", closes=[10.0] * 10, vols=[1000.0] * 10)

    resp = client.get("/market/stocks/trendok?symbols=CN:000001")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list) and len(data) == 1
    row = data[0]
    assert row["symbol"] == "CN:000001"
    assert row["trendOk"] is None
    assert "bars_lt_60" in (row.get("missingData") or [])


def test_watchlist_trendok_pass_and_fail(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.sqlite3"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    client = TestClient(main.app)
    _seed_market_stock("CN:000001", "CN", "000001", "Alpha")
    _seed_market_stock("CN:000002", "CN", "000002", "Beta")

    # Build 70 days closes:
    # - Gentle uptrend with periodic pullbacks to keep RSI in 50..75
    # - A small dip before the last 4 days + clear acceleration to satisfy MACD histogram expansion
    closes: list[float] = []
    price = 10.0
    for i in range(70):
        if i < 56:
            # Early: gentle uptrend with periodic pullbacks.
            if i % 5 == 0 and i > 0:
                price -= 0.04
            else:
                price += 0.05
        elif i < 64:
            # Buffer zone: add a slight negative drift to keep RSI away from overbought,
            # while preserving a higher baseline for EMA order.
            price += 0.04 if (i % 2 == 1) else -0.14
        else:
            # Last 6 days: clear acceleration to make MACD histogram turn positive earlier and keep expanding.
            step = [0.08, 0.10, 0.12, 0.14, 0.16, 0.17][i - 64]
            price += step
        closes.append(round(price, 4))

    # Volumes: last 5 days boosted to satisfy AvgVol(5) > 1.2 * AvgVol(30)
    vols_ok: list[float] = [1000.0] * 65 + [2000.0] * 5
    _seed_market_bars("CN:000001", "2025-10-01", closes=closes, vols=vols_ok)

    # Fail case: same closes but flat volume -> volumeSurge should fail, hence TrendOK false (if other checks computable).
    vols_fail: list[float] = [1000.0] * 70
    _seed_market_bars("CN:000002", "2025-10-01", closes=closes, vols=vols_fail)

    resp = client.get("/market/stocks/trendok?symbols=CN:000001&symbols=CN:000002")
    assert resp.status_code == 200
    rows = resp.json()
    assert isinstance(rows, list) and len(rows) == 2

    r1 = rows[0]
    assert r1["symbol"] == "CN:000001"
    assert r1["trendOk"] in (True, False, None)
    # Show which sub-check fails (if any).
    checks1 = r1.get("checks") or {}
    assert checks1.get("emaOrder") is True
    assert checks1.get("macdPositive") is True
    assert checks1.get("macdHistExpanding") is True, (
        f"macdHist4={((r1.get('values') or {}).get('macdHist4') or [])} "
        f"macd={((r1.get('values') or {}).get('macd'))} "
        f"signal={((r1.get('values') or {}).get('macdSignal'))}"
    )
    assert checks1.get("closeNear20dHigh") is True
    assert checks1.get("rsiInRange") is True, f"rsi14={((r1.get('values') or {}).get('rsi14'))}"
    assert checks1.get("volumeSurge") is True
    assert r1["trendOk"] is True
    assert r1.get("score") is not None
    assert 0 <= float(r1["score"]) <= 100
    assert isinstance(r1.get("scoreParts"), dict)
    assert r1.get("stopLossPrice") is not None
    sl1 = float(r1["stopLossPrice"])
    assert sl1 > 0
    assert sl1 <= float((r1.get("values") or {}).get("close"))
    slp1 = r1.get("stopLossParts") or {}
    assert isinstance(slp1, dict)
    assert "hard_stop" in slp1
    assert sl1 >= float(slp1["hard_stop"])

    r2 = rows[1]
    assert r2["symbol"] == "CN:000002"
    # Volume surge should fail, thus TrendOK should be False when indicators are available.
    assert r2["trendOk"] is False
    assert r2["checks"]["volumeSurge"] is False
    assert r2.get("score") is not None
    assert 0 <= float(r2["score"]) <= 100
    assert r2.get("stopLossPrice") is not None
    sl2 = float(r2["stopLossPrice"])
    assert sl2 > 0
    assert sl2 <= float((r2.get("values") or {}).get("close"))
    slp2 = r2.get("stopLossParts") or {}
    assert isinstance(slp2, dict)
    assert "hard_stop" in slp2
    assert sl2 >= float(slp2["hard_stop"])

    # Pass-case should score higher than fail-case (volume confirmation contributes).
    assert float(r1["score"]) > float(r2["score"])

