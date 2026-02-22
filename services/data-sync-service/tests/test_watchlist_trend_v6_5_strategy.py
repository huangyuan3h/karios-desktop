from __future__ import annotations

import pytest

from data_sync_service.testback.strategies.base import Bar, PortfolioSnapshot
from data_sync_service.testback.strategies.watchlist_momentum_v1_1 import WatchlistTrendV6_5Strategy


def _bar(ts_code: str, trade_date: str, close: float, volume: float = 1_000_000) -> Bar:
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
    strat: WatchlistTrendV6_5Strategy, ts_code: str, start_idx: int, prices: list[float]
) -> None:
    for i, p in enumerate(prices):
        d = f"2026-01-{start_idx + i:02d}"
        strat.on_bar(d, {ts_code: _bar(ts_code, d, p, volume=2_000_000)}, PortfolioSnapshot(0, 1_000_000, {}))


def test_weak_regime_suppresses_new_buys(monkeypatch: pytest.MonkeyPatch) -> None:
    from data_sync_service.testback.strategies import watchlist_momentum_v1_1 as mod

    monkeypatch.setattr(mod, "get_market_regime", lambda as_of_date: {"regime": "Weak"})

    strat = WatchlistTrendV6_5Strategy(min_hold_bars=0)
    _feed_history(strat, "CN:AAA", 1, [10 + i * 0.2 for i in range(60)])

    d = "2026-03-01"
    bars = {"CN:AAA": _bar("CN:AAA", d, 22.0, volume=3_000_000)}
    pf = PortfolioSnapshot(cash=1_000_000.0, equity=1_000_000.0, positions={})

    orders = strat.on_bar(d, bars, pf)
    assert not any(o.action == "buy" for o in orders)


def test_min_hold_bars_prevents_rotation(monkeypatch: pytest.MonkeyPatch) -> None:
    from data_sync_service.testback.strategies import watchlist_momentum_v1_1 as mod

    monkeypatch.setattr(mod, "get_market_regime", lambda as_of_date: {"regime": "Strong"})

    strat = WatchlistTrendV6_5Strategy(min_hold_bars=5, rebalance_threshold_pct=0.0)

    up_a = [10 + i * 0.15 for i in range(60)]
    up_b = [10 + i * 0.25 for i in range(60)]
    _feed_history(strat, "CN:A", 1, up_a)
    _feed_history(strat, "CN:B", 1, up_b)

    # Simulate a recent entry in A (holding bars < min_hold_bars).
    strat._entry_price["CN:A"] = up_a[-1]
    strat._peak_price_since_entry["CN:A"] = up_a[-1]
    strat._entry_index["CN:A"] = strat._bar_index.get("CN:A", 0)

    d = "2026-03-01"
    bars = {
        "CN:A": _bar("CN:A", d, up_a[-1], volume=3_000_000),
        "CN:B": _bar("CN:B", d, up_b[-1], volume=3_000_000),
    }
    pf = PortfolioSnapshot(cash=200_000.0, equity=1_000_000.0, positions={"CN:A": 10_000.0})

    orders = strat.on_bar(d, bars, pf)
    assert not any(o.ts_code == "CN:A" and o.reason == "v6_5_rotate" for o in orders)

