from __future__ import annotations

import pytest

from data_sync_service.testback.strategies.base import Bar, PortfolioSnapshot
from data_sync_service.testback.strategies.watchlist_relvol_momentum_v1 import (
    WatchlistRelVolumeMomentumV2Strategy,
)


def _bar(ts_code: str, trade_date: str, close: float, volume: float) -> Bar:
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
    strat: WatchlistRelVolumeMomentumV2Strategy, ts_code: str, start_idx: int, prices: list[float], volume: float
) -> None:
    for i, p in enumerate(prices):
        d = f"2026-01-{start_idx + i:02d}"
        strat.on_bar(d, {ts_code: _bar(ts_code, d, p, volume)}, PortfolioSnapshot(0, 1_000_000, {}))


def test_weak_regime_blocks_new_buys(monkeypatch: pytest.MonkeyPatch) -> None:
    import data_sync_service.testback.strategies.watchlist_relvol_momentum_v1 as relv

    monkeypatch.setattr(relv, "get_market_regime", lambda as_of_date: {"regime": "Weak"})

    strat = WatchlistRelVolumeMomentumV2Strategy()
    _feed_history(strat, "CN:AAA", 1, [10 + i * 0.1 for i in range(80)], volume=1_000_000)

    d = "2026-03-01"
    bars = {"CN:AAA": _bar("CN:AAA", d, 18.0, 2_000_000)}
    pf = PortfolioSnapshot(cash=1_000_000.0, equity=1_000_000.0, positions={})

    orders = strat.on_bar(d, bars, pf)
    assert not any(o.action == "buy" for o in orders)


def test_hard_stop_triggers_sell(monkeypatch: pytest.MonkeyPatch) -> None:
    import data_sync_service.testback.strategies.watchlist_relvol_momentum_v1 as relv

    monkeypatch.setattr(relv, "get_market_regime", lambda as_of_date: {"regime": "Strong"})

    strat = WatchlistRelVolumeMomentumV2Strategy(hard_stop_loss_pct=0.08, trailing_stop_pct=0.10)
    _feed_history(strat, "CN:AAA", 1, [10.0] * 80, volume=1_000_000)

    strat._entry_price["CN:AAA"] = 10.0
    strat._peak_price_since_entry["CN:AAA"] = 10.0

    d = "2026-03-01"
    bar = _bar("CN:AAA", d, 9.0, 2_000_000)
    pf = PortfolioSnapshot(cash=0.0, equity=1_000_000.0, positions={"CN:AAA": 10_000.0})

    orders = strat.on_bar(d, {"CN:AAA": bar}, pf)
    assert any(o.ts_code == "CN:AAA" and o.action == "sell" for o in orders)
