from data_sync_service.service import watchlist_v5_alerts as alerts  # type: ignore[import-not-found]


def _bars(close: float, high: float, low: float, days: int = 30) -> list[tuple[str, str, str, str, str, str]]:
    out: list[tuple[str, str, str, str, str, str]] = []
    for i in range(days):
        d = f"2024-01-{i + 1:02d}"
        out.append((d, str(close), str(high), str(low), str(close), "1000"))
    return out


def test_v5_alert_buy_add(monkeypatch) -> None:
    def fake_ema(values, period):
        if period == 20:
            return [1.0, 2.0]
        if period == 30:
            return [0.8, 1.5]
        return [1.0, 1.0]

    def fake_macd(values):
        return ([0.1, 0.2], [0.0, 0.0], [0.1, 0.2])

    def fake_rsi(values, period=14):
        return [60.0]

    monkeypatch.setattr(alerts, "_ema", fake_ema)
    monkeypatch.setattr(alerts, "_macd", fake_macd)
    monkeypatch.setattr(alerts, "_rsi", fake_rsi)
    monkeypatch.setattr(alerts, "fetch_last_ohlcv_batch", lambda *_args, **_kwargs: {"000001.SZ": _bars(100, 100, 98)})
    monkeypatch.setattr(alerts, "get_market_regime", lambda **_kwargs: {"regime": "Strong"})

    res = alerts.compute_watchlist_v5_alerts([{"symbol": "CN:000001", "position_pct": 0.0}])
    assert res[0]["action"] == "buy_add"
    assert abs(res[0]["targetPct"] - 0.3333) < 1e-4


def test_v5_alert_exit_on_trend_break(monkeypatch) -> None:
    def fake_ema(values, period):
        if period == 20:
            return [1.0, 1.0]
        if period == 30:
            return [2.0, 2.0]
        return [1.0, 1.0]

    def fake_macd(values):
        return ([-0.1, -0.2], [0.0, 0.0], [-0.1, -0.2])

    def fake_rsi(values, period=14):
        return [60.0]

    monkeypatch.setattr(alerts, "_ema", fake_ema)
    monkeypatch.setattr(alerts, "_macd", fake_macd)
    monkeypatch.setattr(alerts, "_rsi", fake_rsi)
    monkeypatch.setattr(alerts, "fetch_last_ohlcv_batch", lambda *_args, **_kwargs: {"000001.SZ": _bars(100, 100, 98)})
    monkeypatch.setattr(alerts, "get_market_regime", lambda **_kwargs: {"regime": "Strong"})

    res = alerts.compute_watchlist_v5_alerts([{"symbol": "CN:000001", "position_pct": 0.5}])
    assert res[0]["action"] == "exit"
    assert res[0]["targetPct"] == 0.0


def test_v5_alert_trim_in_weak_regime(monkeypatch) -> None:
    def fake_ema(values, period):
        if period == 20:
            return [2.0, 3.0]
        if period == 30:
            return [1.0, 2.0]
        return [2.0, 2.0]

    def fake_macd(values):
        return ([0.1, 0.2], [0.0, 0.0], [0.1, 0.2])

    def fake_rsi(values, period=14):
        return [60.0]

    monkeypatch.setattr(alerts, "_ema", fake_ema)
    monkeypatch.setattr(alerts, "_macd", fake_macd)
    monkeypatch.setattr(alerts, "_rsi", fake_rsi)
    monkeypatch.setattr(alerts, "fetch_last_ohlcv_batch", lambda *_args, **_kwargs: {"000001.SZ": _bars(100, 130, 98)})
    monkeypatch.setattr(alerts, "get_market_regime", lambda **_kwargs: {"regime": "Weak"})

    res = alerts.compute_watchlist_v5_alerts([{"symbol": "CN:000001", "position_pct": 0.8}])
    assert res[0]["action"] == "trim"
    assert abs(res[0]["targetPct"] - 0.3) < 1e-6
