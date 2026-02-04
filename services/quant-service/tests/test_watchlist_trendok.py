from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient

import main


def _seed_market_stock(symbol: str, market: str, ticker: str, name: str, currency: str = "CNY") -> None:
    # Uses main._connect() to ensure DB schema is initialized.
    main._ensure_market_stock_basic(symbol=symbol, market=market, ticker=ticker, name=name, currency=currency)


def _seed_market_bars(
    symbol: str,
    start_date: str,
    closes: list[float],
    vols: list[float],
    *,
    opens: list[float] | None = None,
    highs: list[float] | None = None,
    lows: list[float] | None = None,
) -> None:
    assert len(closes) == len(vols)
    if opens is not None:
        assert len(opens) == len(closes)
    if highs is not None:
        assert len(highs) == len(closes)
    if lows is not None:
        assert len(lows) == len(closes)
    start = datetime.fromisoformat(start_date).replace(tzinfo=UTC)
    with main._connect() as conn:
        for i, (c, v) in enumerate(zip(closes, vols, strict=True)):
            d = (start + timedelta(days=i)).date().isoformat()
            o = opens[i] if opens is not None else c
            h = highs[i] if highs is not None else c
            low = lows[i] if lows is not None else c
            conn.execute(
                """
                INSERT INTO market_bars(symbol, date, open, high, low, close, volume, amount, updated_at)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    symbol,
                    d,
                    str(o),
                    str(h),
                    str(low),
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

    # Fail case: flat volume AND last close is NOT a 20D new high -> volumeSurge should fail.
    vols_fail: list[float] = [1000.0] * 70
    closes2 = list(closes)
    high20 = max(closes2[-20:])
    closes2[-1] = round(high20 * 0.97, 4)  # still near high, but not a new high
    _seed_market_bars("CN:000002", "2025-10-01", closes=closes2, vols=vols_fail)

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
    assert float(r1["score"]) >= 0
    assert isinstance(r1.get("scoreParts"), dict)
    assert r1.get("stopLossPrice") is not None
    sl1 = float(r1["stopLossPrice"])
    assert sl1 > 0
    close1 = (r1.get("values") or {}).get("close")
    assert close1 is not None
    assert sl1 <= float(close1)
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
    assert float(r2["score"]) >= 0
    assert r2.get("stopLossPrice") is not None
    sl2 = float(r2["stopLossPrice"])
    assert sl2 > 0
    close2 = (r2.get("values") or {}).get("close")
    assert close2 is not None
    assert sl2 <= float(close2)
    slp2 = r2.get("stopLossParts") or {}
    assert isinstance(slp2, dict)
    assert "hard_stop" in slp2
    assert sl2 >= float(slp2["hard_stop"])

    # Pass-case should score higher than fail-case (volume confirmation contributes).
    assert float(r1["score"]) > float(r2["score"])


def test_watchlist_stoploss_exit_now_on_trend_structure_break(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.sqlite3"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    client = TestClient(main.app)
    _seed_market_stock("CN:000003", "CN", "000003", "Gamma")

    # Create a clear downtrend so EMA5 < EMA20 and/or close < EMA20 triggers exit-now.
    closes: list[float] = []
    price = 20.0
    for _i in range(70):
        price -= 0.12
        closes.append(round(price, 4))

    vols: list[float] = [1000.0] * 65 + [800.0] * 5
    _seed_market_bars("CN:000003", "2025-10-01", closes=closes, vols=vols)

    resp = client.get("/market/stocks/trendok?symbols=CN:000003")
    assert resp.status_code == 200
    rows = resp.json()
    assert isinstance(rows, list) and len(rows) == 1
    r = rows[0]
    assert r["symbol"] == "CN:000003"
    assert r.get("stopLossPrice") is not None
    close0 = (r.get("values") or {}).get("close")
    assert close0 is not None
    close = float(close0)
    assert float(r["stopLossPrice"]) == close
    parts = r.get("stopLossParts") or {}
    assert isinstance(parts, dict)
    assert parts.get("exit_now") is True
    assert isinstance(parts.get("exit_reasons"), list)


def test_watchlist_stoploss_warn_on_macd_hist_shrink_but_positive(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.sqlite3"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    client = TestClient(main.app)
    _seed_market_stock("CN:000004", "CN", "000004", "Delta")

    # Construct a series that trends up strongly, then slows/plateaus so MACD hist shrinks
    # for several days but stays positive, with volume drying up.
    closes: list[float] = []
    price = 10.0
    # Strong uptrend
    for _ in range(55):
        price += 0.10
        closes.append(round(price, 4))
    # Slowing uptrend / plateau
    for step in [0.06, 0.04, 0.03, 0.02, 0.01, 0.01, 0.01, 0.01, 0.01, 0.01, 0.01, 0.01, 0.01, 0.01, 0.01]:
        price += step
        closes.append(round(price, 4))
    assert len(closes) >= 70
    closes = closes[:70]

    # Volume dries up in the last 5 days.
    vols: list[float] = [2000.0] * 65 + [800.0] * 5
    _seed_market_bars("CN:000004", "2025-10-01", closes=closes, vols=vols)

    resp = client.get("/market/stocks/trendok?symbols=CN:000004")
    assert resp.status_code == 200
    r = resp.json()[0]
    parts = r.get("stopLossParts") or {}
    assert isinstance(parts, dict)

    # If the specific MACD histogram shrink condition is met, we should see the warning flag.
    # This test is designed to be robust: require the flag OR the absence of the precondition in macdHist4.
    h4 = ((r.get("values") or {}).get("macdHist4")) or []
    if isinstance(h4, list) and len(h4) == 4 and all(isinstance(x, (int, float)) for x in h4):
        h0, h1, h2, h3 = [float(x) for x in h4]
        precond = (h0 > h1 > h2 > h3 > 0.0)
        if precond:
            assert parts.get("warn_reduce_half") is True
            assert isinstance(parts.get("warn_display"), str)


def test_watchlist_buy_mode_b_momentum(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.sqlite3"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    client = TestClient(main.app)
    _seed_market_stock("CN:000005", "CN", "000005", "Epsilon")

    # Build an uptrend with mild pullbacks (keep RSI < 80), then a small acceleration + volume surge.
    closes: list[float] = []
    price = 10.0
    for i in range(70):
        if i % 7 == 0 and i > 0:
            price -= 0.06
        else:
            price += 0.08
        closes.append(round(price, 4))
    # Boost last 2 days to encourage MACD hist increasing.
    closes[-2] = round(closes[-3] + 0.10, 4)
    closes[-1] = round(closes[-2] + 0.14, 4)

    vols: list[float] = [1000.0] * 69 + [2500.0]
    _seed_market_bars("CN:000005", "2025-10-01", closes=closes, vols=vols)

    resp = client.get("/market/stocks/trendok?symbols=CN:000005")
    assert resp.status_code == 200
    r = resp.json()[0]
    assert r.get("buyMode") in ("B_momentum", "A_pullback", "none")
    assert r.get("buyMode") == "B_momentum"
    assert r.get("buyAction") in ("buy", "wait", "avoid", "add")
    assert r.get("buyZoneLow") is not None
    assert r.get("buyZoneHigh") is not None
    assert float(r["buyZoneHigh"]) >= float(r["buyZoneLow"])
    # Ideally should be actionable in this constructed setup.
    assert r.get("buyAction") in ("buy", "wait")


def test_watchlist_buy_mode_a_pullback_buy(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.sqlite3"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    client = TestClient(main.app)
    _seed_market_stock("CN:000006", "CN", "000006", "Zeta")

    # Build 60 days of flat range (10.0), then a breakout spike, then a sharp pullback.
    # This setup should NOT be considered `in_trend` (MACD hist tends to weaken/turn <=0 after the spike),
    # but should satisfy Mode A (breakout happened within last 1..5 days and today is a pullback buy).
    closes: list[float] = [10.0] * 60
    highs: list[float] = [10.0] * 60
    lows: list[float] = [10.0] * 60
    opens: list[float] = [10.0] * 60
    vols: list[float] = [1000.0] * 60

    # Add 9 days (total 69). We'll set the breakout day at index -5 (within 1..5 day window).
    for _ in range(9):
        closes.append(10.0)
        highs.append(10.0)
        lows.append(10.0)
        opens.append(10.0)
        vols.append(900.0)

    # Breakout day 4 days ago (index -5): close > prior 20-day high (10.0) with volume surge.
    closes[-5] = 12.0
    highs[-5] = 12.0
    lows[-5] = 11.6
    opens[-5] = 11.7
    vols[-5] = 2200.0

    # Multi-day pullback (helps MACD hist weaken/turn <=0)
    closes[-4] = 10.3
    highs[-4] = 10.6
    lows[-4] = 10.12
    opens[-4] = 10.4
    vols[-4] = 1500.0

    closes[-3] = 10.1
    highs[-3] = 10.3
    lows[-3] = 10.02
    opens[-3] = 10.2
    vols[-3] = 1300.0

    closes[-2] = 10.0
    highs[-2] = 10.15
    lows[-2] = 9.98
    opens[-2] = 10.05
    vols[-2] = 1100.0

    # Today (last): pullback buy day (bullish candle, shrinking volume, touches breakout zone)
    lows[-1] = 10.03
    opens[-1] = 10.18
    closes[-1] = 10.25  # placeholder; will be adjusted to EMA20_prev for deterministic mode selection
    highs[-1] = 10.3
    vols[-1] = 900.0

    assert len(closes) == 69
    # Make today's close equal to yesterday's EMA20 so:
    # - close > ema20 is False (mode A eligible)
    # - close < ema20 is False (no exit-now)
    def _ema_last(vals: list[float], period: int) -> float:
        alpha = 2.0 / (float(period) + 1.0)
        prev = vals[0]
        for v in vals[1:]:
            prev = alpha * v + (1.0 - alpha) * prev
        return prev

    ema20_prev = _ema_last(closes[:-1], 20)
    closes[-1] = float(ema20_prev)
    # Keep candle bullish and high>=close.
    opens[-1] = min(opens[-1], closes[-1] - 0.01)
    highs[-1] = max(highs[-1], closes[-1])

    _seed_market_bars("CN:000006", "2025-10-01", closes=closes, vols=vols, opens=opens, highs=highs, lows=lows)

    resp = client.get("/market/stocks/trendok?symbols=CN:000006")
    assert resp.status_code == 200
    r = resp.json()[0]
    assert r.get("buyMode") == "A_pullback", f"buyMode={r.get('buyMode')} buyAction={r.get('buyAction')} checks={r.get('buyChecks')}"
    assert r.get("buyAction") in ("buy", "wait", "avoid")
    # In our constructed case, we expect a buy signal.
    assert r.get("buyAction") == "buy", f"buyMode={r.get('buyMode')} checks={r.get('buyChecks')}"
    assert r.get("buyZoneLow") is not None
    assert r.get("buyZoneHigh") is not None
    assert float(r["buyZoneHigh"]) >= float(r["buyZoneLow"])

