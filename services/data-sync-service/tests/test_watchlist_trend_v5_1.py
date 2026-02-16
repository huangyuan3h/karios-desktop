from data_sync_service.testback.strategies import watchlist_trend_v5_1 as v5_1  # type: ignore[import-not-found]
from data_sync_service.testback.strategies.base import Bar, PortfolioSnapshot  # type: ignore[import-not-found]
from data_sync_service.testback.strategies.watchlist_trend_v5_1 import (  # type: ignore[import-not-found]
    WatchlistTrendV5_1Strategy,
)


def _bar(
    ts_code: str,
    trade_date: str,
    close: float,
) -> Bar:
    return Bar(
        ts_code=ts_code,
        trade_date=trade_date,
        open=close,
        high=close,
        low=close,
        close=close,
        avg_price=close,
        volume=1000.0,
        amount=0.0,
    )


def test_v5_1_caps_positions_to_four(monkeypatch) -> None:
    def fake_ema(values, period):
        return [10.0, 11.0]

    def fake_macd(values):
        return ([0.1, 0.2], [0.0, 0.0], [0.1, 0.2])

    def fake_rsi(values, period=14):
        return [60.0]

    monkeypatch.setattr(v5_1, "_ema", fake_ema)
    monkeypatch.setattr(v5_1, "_macd", fake_macd)
    monkeypatch.setattr(v5_1, "_rsi", fake_rsi)

    strategy = WatchlistTrendV5_1Strategy(slow_window=2, max_positions=4)
    strategy._get_regime = lambda _d: "Diverging"

    portfolio = PortfolioSnapshot(cash=100.0, equity=100.0, positions={})
    bars = {
        "000001.SZ": _bar("000001.SZ", "2024-01-01", 10.0),
        "000002.SZ": _bar("000002.SZ", "2024-01-01", 10.0),
        "000003.SZ": _bar("000003.SZ", "2024-01-01", 10.0),
        "000004.SZ": _bar("000004.SZ", "2024-01-01", 10.0),
        "000005.SZ": _bar("000005.SZ", "2024-01-01", 10.0),
    }
    orders = strategy.on_bar("2024-01-02", bars, portfolio)
    buy_targets = [o for o in orders if o.action == "buy"]
    assert len(buy_targets) <= 4


def test_v5_1_respects_custom_weights() -> None:
    strategy = WatchlistTrendV5_1Strategy(rank_weights=[0.4, 0.3, 0.2, 0.1])
    targets = strategy._rank_targets(["a", "b", "c", "d"], 1.0)
    assert abs(targets["a"] - 0.4) < 1e-6
    assert abs(targets["b"] - 0.3) < 1e-6
    assert abs(targets["c"] - 0.2) < 1e-6
    assert abs(targets["d"] - 0.1) < 1e-6
