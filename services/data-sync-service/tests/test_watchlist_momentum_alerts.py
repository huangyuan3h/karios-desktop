from data_sync_service.service import watchlist_momentum_alerts as alerts  # type: ignore[import-not-found]


def _bars(close: float, high: float, low: float, days: int = 60) -> list[tuple[str, str, str, str, str, str]]:
    out: list[tuple[str, str, str, str, str, str]] = []
    for i in range(days):
        d = f"2024-01-{i + 1:02d}"
        out.append((d, str(close), str(high), str(low), str(close), "1000"))
    return out


def test_momentum_plan_returns_holdings(monkeypatch) -> None:
    def fake_ema(values, period):
        return [10.0, 11.0]

    def fake_macd(values):
        return ([0.1, 0.2], [0.0, 0.0], [0.1, 0.2])

    def fake_rsi(values, period=14):
        return [60.0]

    monkeypatch.setattr(alerts, "_ema", fake_ema)
    monkeypatch.setattr(alerts, "_macd", fake_macd)
    monkeypatch.setattr(alerts, "_rsi", fake_rsi)
    monkeypatch.setattr(alerts, "fetch_last_ohlcv_batch", lambda *_args, **_kwargs: {"000001.SZ": _bars(100, 110, 98)})
    monkeypatch.setattr(alerts, "get_market_regime", lambda **_kwargs: {"regime": "Strong"})

    plan = alerts.compute_watchlist_momentum_plan([{"symbol": "CN:000001", "position_pct": 0.2}])
    assert plan["holdings"]
