from __future__ import annotations

import pytest

from data_sync_service.testback.strategies.base import Bar, PortfolioSnapshot
from data_sync_service.testback.strategies.watchlist_trend_v9 import WatchlistTrendV9Strategy


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
    strat: WatchlistTrendV9Strategy, ts_code: str, start_idx: int, prices: list[float]
) -> None:
    for i, p in enumerate(prices):
        d = f"2026-01-{start_idx + i:02d}"
        strat.on_bar(d, {ts_code: _bar(ts_code, d, p)}, PortfolioSnapshot(0, 1_000_000, {}))


def test_weak_regime_blocks_new_buys(monkeypatch: pytest.MonkeyPatch) -> None:
    import data_sync_service.testback.strategies.watchlist_trend_v9 as v9

    monkeypatch.setattr(v9, "get_market_regime", lambda as_of_date: {"regime": "Weak"})

    strat = WatchlistTrendV9Strategy()
    _feed_history(strat, "CN:AAA", 1, [10 + i * 0.10 for i in range(200)])

    d = "2026-09-01"
    bars = {"CN:AAA": _bar("CN:AAA", d, 30.0, volume=4_000_000)}
    pf = PortfolioSnapshot(cash=1_000_000.0, equity=1_000_000.0, positions={})

    orders = strat.on_bar(d, bars, pf)
    assert not any(o.action == "buy" for o in orders)


def test_hard_stop_triggers_sell(monkeypatch: pytest.MonkeyPatch) -> None:
    import data_sync_service.testback.strategies.watchlist_trend_v9 as v9

    monkeypatch.setattr(v9, "get_market_regime", lambda as_of_date: {"regime": "Strong"})

    strat = WatchlistTrendV9Strategy(hard_stop_loss_pct=0.07, trailing_stop_pct=0.10)
    _feed_history(strat, "CN:AAA", 1, [10.0] * 200)

    strat._entry_price["CN:AAA"] = 10.0
    strat._peak_price_since_entry["CN:AAA"] = 10.0

    d = "2026-09-01"
    bar = _bar("CN:AAA", d, 9.2, volume=2_000_000)
    pf = PortfolioSnapshot(cash=0.0, equity=1_000_000.0, positions={"CN:AAA": 10_000.0})

    orders = strat.on_bar(d, {"CN:AAA": bar}, pf)
    assert any(o.ts_code == "CN:AAA" and o.action == "sell" for o in orders)


def test_min_hold_bars_prevents_rotation(monkeypatch: pytest.MonkeyPatch) -> None:
    import data_sync_service.testback.strategies.watchlist_trend_v9 as v9

    monkeypatch.setattr(v9, "get_market_regime", lambda as_of_date: {"regime": "Strong"})

    strat = WatchlistTrendV9Strategy(min_hold_bars=10, rebalance_threshold_pct=0.0)
    up_a = [10 + i * 0.2 for i in range(200)]
    up_b = [10 + i * 0.25 for i in range(200)]
    _feed_history(strat, "CN:A", 1, up_a)
    _feed_history(strat, "CN:B", 1, up_b)

    strat._entry_price["CN:A"] = up_a[-1]
    strat._peak_price_since_entry["CN:A"] = up_a[-1]
    strat._entry_index["CN:A"] = strat._bar_index.get("CN:A", 0)

    d = "2026-09-01"
    bars = {
        "CN:A": _bar("CN:A", d, up_a[-1]),
        "CN:B": _bar("CN:B", d, up_b[-1]),
    }
    pf = PortfolioSnapshot(cash=200_000.0, equity=1_000_000.0, positions={"CN:A": 10_000.0})

    orders = strat.on_bar(d, bars, pf)
    assert not any(o.ts_code == "CN:A" and o.reason == "v9_rotate" for o in orders)
