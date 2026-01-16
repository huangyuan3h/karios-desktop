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
    # - Last 4 days have enough momentum expansions to satisfy MACD histogram expanding (best-effort)
    closes: list[float] = []
    price = 10.0
    for i in range(70):
        if i < 56:
            # Early: gentle uptrend with periodic pullbacks.
            if i % 5 == 0 and i > 0:
                price -= 0.04
            else:
                price += 0.05
        else:
            # Last ~14 days: explicit alternating moves to keep RSI in-range.
            if i < 67:
                price += 0.06 if (i % 2 == 1) else -0.06
            else:
                # Last 3 days: mild acceleration.
                if i == 67:
                    price += 0.06
                elif i == 68:
                    price += 0.08
                else:
                    price += 0.10
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
    assert checks1.get("macdHistExpanding") is True
    assert checks1.get("closeNear20dHigh") is True
    assert checks1.get("rsiInRange") is True
    assert checks1.get("volumeSurge") is True
    assert r1["trendOk"] is True

    r2 = rows[1]
    assert r2["symbol"] == "CN:000002"
    # Volume surge should fail, thus TrendOK should be False when indicators are available.
    assert r2["trendOk"] is False
    assert r2["checks"]["volumeSurge"] is False

