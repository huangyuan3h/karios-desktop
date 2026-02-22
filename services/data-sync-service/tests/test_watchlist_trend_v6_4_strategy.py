from __future__ import annotations

import pytest

from data_sync_service.testback.strategies.base import Bar, PortfolioSnapshot
from data_sync_service.testback.strategies.watchlist_momentum_v1_1 import WatchlistTrendV6_4Strategy


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
    strat: WatchlistTrendV6_4Strategy, ts_code: str, start_idx: int, prices: list[float]
) -> None:
    for i, p in enumerate(prices):
        d = f"2026-01-{start_idx + i:02d}"
        strat.on_bar(d, {ts_code: _bar(ts_code, d, p, volume=2_000_000)}, PortfolioSnapshot(0, 1_000_000, {}))


def test_hard_stop_triggers_sell(monkeypatch: pytest.MonkeyPatch) -> None:
    # Force a stable regime so sizing does not interfere.
    from data_sync_service.testback.strategies import watchlist_momentum_v1_1 as mod

    monkeypatch.setattr(mod, "get_market_regime", lambda as_of_date: {"regime": "Strong"})

    strat = WatchlistTrendV6_4Strategy(hard_stop_loss_pct=0.10, trailing_stop_pct=0.10, rebalance_threshold_pct=0.0)

    # Build enough history for indicator windows.
    _feed_history(strat, "CN:000001", 1, [10.0] * 35)

    # Simulate an existing holding with entry at 10.0.
    strat._entry_price["CN:000001"] = 10.0
    strat._peak_price_since_entry["CN:000001"] = 10.0

    d = "2026-02-20"
    bar = _bar("CN:000001", d, 8.9, volume=2_000_000)  # -11% from entry -> should stop out
    pf = PortfolioSnapshot(cash=0.0, equity=1_000_000.0, positions={"CN:000001": 10_000.0})

    orders = strat.on_bar(d, {"CN:000001": bar}, pf)
    assert any(o.ts_code == "CN:000001" and o.action == "sell" and (o.target_pct or 0.0) == 0.0 for o in orders)


def test_concentration_respects_max_positions(monkeypatch: pytest.MonkeyPatch) -> None:
    from data_sync_service.testback.strategies import watchlist_momentum_v1_1 as mod

    monkeypatch.setattr(mod, "get_market_regime", lambda as_of_date: {"regime": "Strong"})

    strat = WatchlistTrendV6_4Strategy(max_positions_strong=2, rebalance_threshold_pct=0.0)

    # Create two clearly stronger momentum series.
    up = [10 + i * 0.2 for i in range(35)]
    flat = [10.0] * 35
    _feed_history(strat, "CN:A", 1, up)
    _feed_history(strat, "CN:B", 1, [10 + i * 0.15 for i in range(35)])
    _feed_history(strat, "CN:C", 1, flat)

    d = "2026-02-20"
    bars = {
        "CN:A": _bar("CN:A", d, up[-1], volume=3_000_000),
        "CN:B": _bar("CN:B", d, 10 + 34 * 0.15, volume=3_000_000),
        "CN:C": _bar("CN:C", d, 10.0, volume=3_000_000),
    }
    pf = PortfolioSnapshot(cash=1_000_000.0, equity=1_000_000.0, positions={})
    orders = strat.on_bar(d, bars, pf)

    # Only 2 names should be targeted for non-zero allocation (max_positions_strong=2).
    targeted = [o for o in orders if o.target_pct is not None and o.target_pct > 0]
    assert len({o.ts_code for o in targeted}) <= 2


def test_rebalance_threshold_suppresses_micro_adjustments(monkeypatch: pytest.MonkeyPatch) -> None:
    from data_sync_service.testback.strategies import watchlist_momentum_v1_1 as mod

    monkeypatch.setattr(mod, "get_market_regime", lambda as_of_date: {"regime": "Strong"})

    strat = WatchlistTrendV6_4Strategy(
        max_positions_strong=2,
        invested_ratio_strong=1.0,
        rebalance_threshold_pct=0.05,
    )

    up = [10 + i * 0.2 for i in range(35)]
    _feed_history(strat, "CN:A", 1, up)
    _feed_history(strat, "CN:B", 1, up)

    d = "2026-02-20"
    bars = {
        "CN:A": _bar("CN:A", d, up[-1], volume=3_000_000),
        "CN:B": _bar("CN:B", d, up[-1], volume=3_000_000),
    }

    # Target per name is 50%. Create a holding at 48% -> delta 2% < threshold -> no order expected for that name.
    equity = 1_000_000.0
    price = up[-1]
    qty_for_48pct = (equity * 0.48) / price
    pf = PortfolioSnapshot(cash=520_000.0, equity=equity, positions={"CN:A": qty_for_48pct})

    orders = strat.on_bar(d, bars, pf)
    assert not any(o.ts_code == "CN:A" and (o.reason or "").startswith("v6_4_rebalance") for o in orders)

