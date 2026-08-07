"""OPT-062 tests: paper cost model (per-market round-trip costs).

Pure functions, no DB. The numbers pin the default assumptions so an
operator changing a bps constant must consciously re-baseline these tests.
"""

from __future__ import annotations

import pytest

from data_sync_service.service.paper_cost_model import (
    MARKET_CN,
    MARKET_HK,
    MARKETS,
    net_pnl_pct,
    round_trip_cost_pct,
)


def test_markets_include_cn_and_hk() -> None:
    assert set(MARKETS) == {MARKET_CN, MARKET_HK}


def test_cn_round_trip_cost() -> None:
    # 万2.5×2 commission + 5bps sell stamp + 10bps×2 slippage = 30 bps.
    assert round_trip_cost_pct(MARKET_CN) == pytest.approx(0.0030)


def test_hk_round_trip_cost() -> None:
    # 5bps×2 commission + 10bps×2 stamp + 15bps×2 slippage = 60 bps.
    assert round_trip_cost_pct(MARKET_HK) == pytest.approx(0.0060)


def test_unknown_market_raises() -> None:
    with pytest.raises(ValueError):
        round_trip_cost_pct("US")


def test_net_pnl_deducts_cn_cost() -> None:
    assert net_pnl_pct(5.0, MARKET_CN) == pytest.approx(4.7)
    assert net_pnl_pct(-2.0, MARKET_CN) == pytest.approx(-2.3)


def test_net_pnl_deducts_hk_cost() -> None:
    assert net_pnl_pct(5.0, MARKET_HK) == pytest.approx(4.4)
    assert net_pnl_pct(-7.0, MARKET_HK) == pytest.approx(-7.6)


def test_costs_are_conservative_and_documented() -> None:
    """Default model must be non-zero for both markets — a zero-cost model
    would silently reintroduce the v0.1 optimistic bias."""
    for market in MARKETS:
        assert round_trip_cost_pct(market) > 0
