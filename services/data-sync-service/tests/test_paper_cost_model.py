"""OPT-062 tests: paper cost model (per-market round-trip costs).

Pure functions, no DB. The numbers pin the strict realistic assumptions so an
operator changing a bps constant must consciously re-baseline these tests.
"""

from __future__ import annotations

import pytest

from data_sync_service.service.paper_cost_model import (
    MARKET_CN,
    MARKET_HK,
    MARKETS,
    entry_cost_frac,
    exit_cost_frac,
    explicit_round_trip_cost_pct,
    net_pnl_pct,
    round_trip_cost_pct,
    round_trip_cost_pct_at,
    slippage_frac,
    tick_size_for,
)


def test_markets_include_cn_and_hk() -> None:
    assert set(MARKETS) == {MARKET_CN, MARKET_HK}


def test_cn_explicit_round_trip_cost() -> None:
    # 万3x2 commission + 5bps sell stamp + 0.1x2 transfer + 0.541x2 levies
    # = 12.282 bps explicit.
    assert explicit_round_trip_cost_pct(MARKET_CN) == pytest.approx(0.0012282)


def test_cn_round_trip_cost_static_includes_base_slippage() -> None:
    # Explicit 12.282 + 10bpsx2 slippage = 32.282 bps.
    assert round_trip_cost_pct(MARKET_CN) == pytest.approx(0.0032282)


def test_hk_explicit_round_trip_cost() -> None:
    # 20x2 commission + 10x2 stamp + 1.27x2 levies = 62.54 bps explicit.
    assert explicit_round_trip_cost_pct(MARKET_HK) == pytest.approx(0.006254)


def test_hk_round_trip_cost_static_includes_base_slippage() -> None:
    # Explicit 62.54 + 15bpsx2 slippage = 92.54 bps.
    assert round_trip_cost_pct(MARKET_HK) == pytest.approx(0.009254)


def test_unknown_market_raises() -> None:
    with pytest.raises(ValueError):
        round_trip_cost_pct("US")


def test_net_pnl_deducts_cn_cost() -> None:
    assert net_pnl_pct(5.0, MARKET_CN) == pytest.approx(4.67718)
    assert net_pnl_pct(-2.0, MARKET_CN) == pytest.approx(-2.32282)


def test_net_pnl_deducts_hk_cost() -> None:
    assert net_pnl_pct(5.0, MARKET_HK) == pytest.approx(4.0746)
    assert net_pnl_pct(-7.0, MARKET_HK) == pytest.approx(-7.9254)


def test_entry_cost_frac_hk() -> None:
    # Entry side: 20 commission + 10 stamp + 1.27 levies + 15 slippage = 46.27 bps.
    assert entry_cost_frac(MARKET_HK) == pytest.approx(0.004627)
    assert entry_cost_frac(MARKET_CN) == pytest.approx(0.0013641)


def test_exit_cost_frac_cn_includes_sell_stamp() -> None:
    # CN exit: 3 commission + 5 stamp + 0.1 transfer + 0.541 levies + 10 slip.
    assert exit_cost_frac(MARKET_CN) == pytest.approx(0.0018641)


def test_cn_tick_floor_raises_slippage_for_cheap_names() -> None:
    # ¥5 stock: one tick 0.01 / 5 = 20bps > base 10bps.
    assert tick_size_for(MARKET_CN, 5.0) == pytest.approx(0.01)
    assert slippage_frac(MARKET_CN, 5.0) == pytest.approx(0.0020)
    # ¥20 stock: one tick 0.01 / 20 = 5bps < base 10bps -> base applies.
    assert slippage_frac(MARKET_CN, 20.0) == pytest.approx(0.0010)
    assert round_trip_cost_pct_at(MARKET_CN, 5.0, 5.0) == pytest.approx(0.0052282)


def test_hk_tick_table_bands() -> None:
    assert tick_size_for(MARKET_HK, 0.2) == pytest.approx(0.001)
    assert tick_size_for(MARKET_HK, 5.0) == pytest.approx(0.010)
    assert tick_size_for(MARKET_HK, 15.0) == pytest.approx(0.020)
    assert tick_size_for(MARKET_HK, 480.0) == pytest.approx(0.200)


def test_costs_are_conservative_and_documented() -> None:
    """Default model must be non-zero for both markets — a zero-cost model
    would silently reintroduce the v0.1 optimistic bias."""
    for market in MARKETS:
        assert round_trip_cost_pct(market) > 0
        assert explicit_round_trip_cost_pct(market) > 0
