from data_sync_service.testback.strategies import watchlist_trend_v6 as v6  # type: ignore[import-not-found]
from data_sync_service.testback.strategies.base import Bar, PortfolioSnapshot  # type: ignore[import-not-found]
from data_sync_service.testback.strategies.watchlist_trend_v6 import (  # type: ignore[import-not-found]
    WatchlistTrendV6Strategy,
)


def _bar(
    ts_code: str,
    trade_date: str,
    open_price: float,
    high: float,
    low: float,
    close: float,
    volume: float = 1000.0,
) -> Bar:
    avg_price = (open_price + high + low + close) / 4.0
    return Bar(
        ts_code=ts_code,
        trade_date=trade_date,
        open=open_price,
        high=high,
        low=low,
        close=close,
        avg_price=avg_price,
        volume=volume,
        amount=0.0,
    )


def test_v6_stop_price_prefers_tightest() -> None:
    strategy = WatchlistTrendV6Strategy(stop_loss_pct=0.1, atr_stop_mult=2.0, trailing_atr_mult=2.0)
    stop_price = strategy._stop_price(entry_price=100.0, entry_atr=5.0, peak_price=120.0, atr_now=4.0)
    assert stop_price == 112.0


def test_v6_atr_calculation() -> None:
    strategy = WatchlistTrendV6Strategy(atr_window=3)
    history = strategy._history["000001.SZ"]
    history.append(_bar("000001.SZ", "2024-01-01", 10.0, 11.0, 9.0, 10.0))
    history.append(_bar("000001.SZ", "2024-01-02", 11.0, 12.0, 10.0, 11.0))
    history.append(_bar("000001.SZ", "2024-01-03", 10.5, 11.0, 9.5, 10.5))
    history.append(_bar("000001.SZ", "2024-01-04", 11.5, 13.0, 11.0, 12.0))
    atr_val = strategy._calc_atr(history)
    assert abs(atr_val - 2.0) < 1e-6


def test_v6_pullback_requires_recent_breakout(monkeypatch) -> None:
    def fake_ema(values, period):
        if period == 2:
            return [100.0, 100.0]
        if period == 3:
            return [100.0, 100.0]
        if period == 4:
            return [99.0, 99.0]
        return [100.0, 100.0]

    def fake_macd(values):
        return ([0.1, 0.1], [0.0, 0.0], [0.1, 0.1])

    def fake_rsi(values, period=14):
        return [55.0]

    monkeypatch.setattr(v6, "_ema", fake_ema)
    monkeypatch.setattr(v6, "_macd", fake_macd)
    monkeypatch.setattr(v6, "_rsi", fake_rsi)

    strategy = WatchlistTrendV6Strategy(
        fast_window=2,
        mid_window=3,
        slow_window=4,
        cooldown_bars=0,
        pullback_window=1,
        min_trend_strength=0.0,
        breakout_vol_ratio=1.0,
        pullback_vol_ratio=1.0,
    )
    strategy._get_regime = lambda _d: "Strong"

    portfolio = PortfolioSnapshot(cash=100.0, equity=100.0, positions={})
    bars = [
        _bar("000001.SZ", "2024-01-01", 100.0, 110.0, 99.0, 100.0),
        _bar("000001.SZ", "2024-01-02", 100.0, 110.0, 99.0, 100.0),
        _bar("000001.SZ", "2024-01-03", 100.0, 110.0, 99.0, 100.0),
        _bar("000001.SZ", "2024-01-04", 100.0, 110.0, 99.0, 100.0),
    ]
    for i, bar in enumerate(bars):
        orders = strategy.on_bar(f"2024-01-0{i+1}", {"000001.SZ": bar}, portfolio)
    assert orders == []


def test_v6_pullback_allows_after_breakout(monkeypatch) -> None:
    def fake_ema(values, period):
        if period == 2:
            return [100.0, 100.0]
        if period == 3:
            return [100.0, 100.0]
        if period == 4:
            return [99.0, 99.0]
        return [100.0, 100.0]

    def fake_macd(values):
        return ([0.1, 0.1], [0.0, 0.0], [0.1, 0.1])

    def fake_rsi(values, period=14):
        return [60.0]

    monkeypatch.setattr(v6, "_ema", fake_ema)
    monkeypatch.setattr(v6, "_macd", fake_macd)
    monkeypatch.setattr(v6, "_rsi", fake_rsi)

    strategy = WatchlistTrendV6Strategy(
        fast_window=2,
        mid_window=3,
        slow_window=4,
        cooldown_bars=0,
        pullback_window=2,
        min_trend_strength=0.0,
        breakout_vol_ratio=1.0,
        pullback_vol_ratio=1.0,
    )
    strategy._get_regime = lambda _d: "Strong"

    portfolio = PortfolioSnapshot(cash=100.0, equity=100.0, positions={})
    bars = [
        _bar("000001.SZ", "2024-01-01", 100.0, 100.0, 99.0, 100.0),
        _bar("000001.SZ", "2024-01-02", 100.0, 100.0, 99.0, 100.0),
        _bar("000001.SZ", "2024-01-03", 100.0, 100.0, 99.0, 100.0),
        _bar("000001.SZ", "2024-01-04", 100.0, 100.0, 99.0, 100.0),
        _bar("000001.SZ", "2024-01-05", 100.0, 110.0, 99.0, 100.0),
    ]
    for i, bar in enumerate(bars):
        orders = strategy.on_bar(f"2024-01-0{i+1}", {"000001.SZ": bar}, portfolio)
    assert any(o.reason == "pullback tranche" for o in orders)
