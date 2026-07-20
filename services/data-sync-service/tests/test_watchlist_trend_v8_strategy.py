from __future__ import annotations

import pytest

from data_sync_service.testback.strategies.base import Bar, PortfolioSnapshot
from data_sync_service.testback.strategies.watchlist_trend_v8 import WatchlistTrendV8Strategy


def _bar(ts_code: str, trade_date: str, close: float, volume: float = 2_000_000) -> Bar:
    return Bar(
        ts_code=ts_code,
        trade_date=trade_date,
        open=close,
        high=close,
        low=close,
        close=close,
        avg_price=close,
        volume=volume,
        amount=close * volume,
    )


def _feed_history(
    strat: WatchlistTrendV8Strategy, ts_code: str, start_idx: int, prices: list[float]
) -> None:
    for i, p in enumerate(prices):
        d = f"2026-01-{start_idx + i:02d}"
        strat.on_bar(d, {ts_code: _bar(ts_code, d, p)}, PortfolioSnapshot(0, 1_000_000, {}))


def test_weak_regime_blocks_new_buys(monkeypatch: pytest.MonkeyPatch) -> None:
    import data_sync_service.testback.strategies.watchlist_trend_v8 as v8

    monkeypatch.setattr(v8, "get_market_regime", lambda as_of_date: {"regime": "Weak"})

    strat = WatchlistTrendV8Strategy()
    _feed_history(strat, "CN:AAA", 1, [10 + i * 0.12 for i in range(160)])

    d = "2026-08-01"
    bars = {"CN:AAA": _bar("CN:AAA", d, 29.0, volume=4_000_000)}
    pf = PortfolioSnapshot(cash=1_000_000.0, equity=1_000_000.0, positions={})

    orders = strat.on_bar(d, bars, pf)
    assert not any(o.action == "buy" for o in orders)


def test_hard_stop_triggers_sell(monkeypatch: pytest.MonkeyPatch) -> None:
    import data_sync_service.testback.strategies.watchlist_trend_v8 as v8

    monkeypatch.setattr(v8, "get_market_regime", lambda as_of_date: {"regime": "Strong"})

    strat = WatchlistTrendV8Strategy(hard_stop_loss_pct=0.085, trailing_stop_pct=0.12)
    _feed_history(strat, "CN:AAA", 1, [10.0] * 160)

    strat._entry_price["CN:AAA"] = 10.0
    strat._peak_price_since_entry["CN:AAA"] = 10.0

    d = "2026-08-01"
    bar = _bar("CN:AAA", d, 9.0, volume=2_000_000)
    pf = PortfolioSnapshot(cash=0.0, equity=1_000_000.0, positions={"CN:AAA": 10_000.0})

    orders = strat.on_bar(d, {"CN:AAA": bar}, pf)
    assert any(o.ts_code == "CN:AAA" and o.action == "sell" for o in orders)


def test_min_hold_bars_prevents_rotation(monkeypatch: pytest.MonkeyPatch) -> None:
    import data_sync_service.testback.strategies.watchlist_trend_v8 as v8

    monkeypatch.setattr(v8, "get_market_regime", lambda as_of_date: {"regime": "Strong"})

    strat = WatchlistTrendV8Strategy(min_hold_bars=10, rebalance_threshold_pct=0.0)
    up_a = [10 + i * 0.2 for i in range(160)]
    up_b = [10 + i * 0.25 for i in range(160)]
    _feed_history(strat, "CN:A", 1, up_a)
    _feed_history(strat, "CN:B", 1, up_b)

    strat._entry_price["CN:A"] = up_a[-1]
    strat._peak_price_since_entry["CN:A"] = up_a[-1]
    strat._entry_index["CN:A"] = strat._bar_index.get("CN:A", 0)

    d = "2026-08-01"
    bars = {
        "CN:A": _bar("CN:A", d, up_a[-1]),
        "CN:B": _bar("CN:B", d, up_b[-1]),
    }
    pf = PortfolioSnapshot(cash=200_000.0, equity=1_000_000.0, positions={"CN:A": 10_000.0})

    orders = strat.on_bar(d, bars, pf)
    assert not any(o.ts_code == "CN:A" and o.reason == "v8_rotate" for o in orders)


def test_strategy_init_defaults():
    strategy = WatchlistTrendV8Strategy()
    assert strategy.fast_window == 10
    assert strategy.mid_window == 20
    assert strategy.slow_window == 60
    assert strategy.long_window == 120
    assert strategy.hard_stop_loss_pct == 0.085
    assert strategy.trailing_stop_pct == 0.12


def test_strategy_init_custom():
    strategy = WatchlistTrendV8Strategy(
        fast_window=5,
        mid_window=15,
        slow_window=40,
        long_window=80,
        hard_stop_loss_pct=0.05,
        trailing_stop_pct=0.10,
    )
    assert strategy.fast_window == 5
    assert strategy.mid_window == 15
    assert strategy.slow_window == 40
    assert strategy.long_window == 80


def test_strategy_window_clamping():
    strategy = WatchlistTrendV8Strategy(fast_window=-1, mid_window=0, slow_window=1, long_window=2)
    assert strategy.fast_window >= 2
    assert strategy.mid_window >= strategy.fast_window + 1


def test_strategy_stop_loss_clamping():
    strategy = WatchlistTrendV8Strategy(hard_stop_loss_pct=1.0, trailing_stop_pct=2.0)
    assert strategy.hard_stop_loss_pct <= 0.50
    assert strategy.trailing_stop_pct <= 0.50


def test_strategy_stop_loss_min():
    strategy = WatchlistTrendV8Strategy(hard_stop_loss_pct=0.001, trailing_stop_pct=0.001)
    assert strategy.hard_stop_loss_pct >= 0.01
    assert strategy.trailing_stop_pct >= 0.02


def test_strategy_invested_ratio_clamping():
    strategy = WatchlistTrendV8Strategy(
        invested_ratio_strong=2.0,
        invested_ratio_diverging=-0.5,
        invested_ratio_weak=1.5,
    )
    assert 0.0 <= strategy.invested_ratio_strong <= 1.0
    assert 0.0 <= strategy.invested_ratio_diverging <= 1.0
    assert 0.0 <= strategy.invested_ratio_weak <= 1.0


def test_strategy_max_positions():
    strategy = WatchlistTrendV8Strategy(
        max_positions_strong=-1,
        max_positions_diverging=0,
        max_positions_weak=10,
    )
    assert strategy.max_positions_strong >= 0
    assert strategy.max_positions_diverging >= 0
    assert strategy.max_positions_weak >= 0


def test_stddev_empty():
    assert WatchlistTrendV8Strategy._stddev([]) == 0.0


def test_stddev_single():
    result = WatchlistTrendV8Strategy._stddev([5.0])
    assert result == 0.0


def test_stddev_multiple():
    result = WatchlistTrendV8Strategy._stddev([2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0])
    assert result > 0
    assert abs(result - 2.0) < 0.01


def test_regime_params_strong():
    strategy = WatchlistTrendV8Strategy()
    max_pos, invested, min_score, allow = strategy._regime_params("Strong")
    assert max_pos == strategy.max_positions_strong
    assert invested == strategy.invested_ratio_strong
    assert allow is True


def test_regime_params_diverging():
    strategy = WatchlistTrendV8Strategy()
    max_pos, invested, min_score, allow = strategy._regime_params("Diverging")
    assert max_pos == strategy.max_positions_diverging
    assert invested == strategy.invested_ratio_diverging
    assert allow is True


def test_regime_params_weak():
    strategy = WatchlistTrendV8Strategy()
    max_pos, invested, min_score, allow = strategy._regime_params("Weak")
    assert max_pos == strategy.max_positions_weak
    assert allow is False


def test_regime_params_unknown():
    strategy = WatchlistTrendV8Strategy()
    max_pos, invested, min_score, allow = strategy._regime_params("Unknown")
    assert allow is False


def test_score_empty():
    strategy = WatchlistTrendV8Strategy()
    assert strategy._score([], []) == 0.0


def test_score_basic():
    strategy = WatchlistTrendV8Strategy()
    closes = [100.0] * 121
    closes[-1] = 110.0
    vols = [1000.0] * 121
    result = strategy._score(closes, vols)
    assert isinstance(result, float)


def test_default_score_config():
    config = WatchlistTrendV8Strategy.default_score_config()
    assert config.top_n == 600
    assert config.momentum_weight == 1.0


def test_min_hold_bars_clamping():
    strategy = WatchlistTrendV8Strategy(min_hold_bars=-5)
    assert strategy.min_hold_bars >= 0


def test_rebalance_threshold_clamping():
    strategy = WatchlistTrendV8Strategy(rebalance_threshold_pct=1.0)
    assert strategy.rebalance_threshold_pct <= 0.30
    strategy2 = WatchlistTrendV8Strategy(rebalance_threshold_pct=-0.1)
    assert strategy2.rebalance_threshold_pct >= 0.0
