from data_sync_service.testback.strategies import watchlist_momentum_v1_1 as v1_1  # type: ignore[import-not-found]
from data_sync_service.testback.strategies.base import Bar, PortfolioSnapshot  # type: ignore[import-not-found]
from data_sync_service.testback.strategies.watchlist_momentum_v1_1 import (  # type: ignore[import-not-found]
    MomentumRankStrategyV1_1,
)


def _bar(ts_code: str, trade_date: str, close: float) -> Bar:
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


def test_v1_1_quality_momentum_positive_for_uptrend() -> None:
    strategy = MomentumRankStrategyV1_1(momentum_window=10)
    closes = [10.0 + i * 0.2 for i in range(10)]
    score = strategy._quality_momentum(closes)
    assert score > 0


def test_v1_1_buys_top_rank(monkeypatch) -> None:
    def fake_ema(values, period):
        return [10.0, 11.0]

    def fake_macd(values):
        return ([0.1, 0.2], [0.0, 0.0], [0.1, 0.2])

    def fake_rsi(values, period=14):
        return [60.0]

    monkeypatch.setattr(v1_1, "_ema", fake_ema)
    monkeypatch.setattr(v1_1, "_macd", fake_macd)
    monkeypatch.setattr(v1_1, "_rsi", fake_rsi)

    strategy = MomentumRankStrategyV1_1(slow_window=2, max_positions=1, momentum_window=10)
    strategy._get_regime = lambda _d: "Strong"

    portfolio = PortfolioSnapshot(cash=100.0, equity=100.0, positions={})
    bars = {
        "000001.SZ": _bar("000001.SZ", "2024-01-01", 10.0),
        "000002.SZ": _bar("000002.SZ", "2024-01-01", 10.0),
    }
    orders = strategy.on_bar("2024-01-02", bars, portfolio)
    assert any(o.action == "buy" for o in orders)
