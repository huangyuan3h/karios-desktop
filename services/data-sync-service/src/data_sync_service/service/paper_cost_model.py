"""Paper-trading cost model (OPT-062 / L3-P1; strict realistic recalibration 2026-09-21).

Per-market round-trip cost estimation, shared by the paper-trading book **and**
the backtest engine (single source: OPT-154 / OPT-172). The model is
deliberately conservative: every recurring fee a real broker/regulator charges
is represented, and slippage carries a **one-tick floor** so cheap names pay
proportionally more.

Cost components applied to a round trip (entry + exit):

- **commission**: broker fee per side (bps of notional), floored at the
  broker's per-trade minimum expressed at a reference book (see below).
- **stamp tax**: CN pays on SELL only (0.05% since 2023-08); HK pays BOTH
  sides (0.1%).
- **transfer fee** (CN 过户费): 0.001% per side.
- **regulatory levies**: CN 经手费 (0.00341%) + 证管费 (0.002%); HK
  SFC (0.0027%) + HKEX (0.00565%) + AFRC (0.00015%) + CCASS (0.0042%).
- **slippage**: adverse price move between signal and fill, per side, modelled
  as ``max(base_bps, one_tick / price)``.

The model has no FX conversion (HK PnL is reported in HKD). ETF parking /
B3 / transfer legs use their own 5bp constant, not this stock model.

Exposed helpers:

- :func:`round_trip_cost_pct` — round-trip cost as a fraction of the position.
- :func:`round_trip_cost_pct_at` — same, with entry/exit prices (tick floor).
- :func:`entry_cost_frac` / :func:`exit_cost_frac` — one-side cost fraction.
- :func:`explicit_round_trip_cost_pct` — fees only, no slippage.
- :func:`slippage_frac` — one-side slippage fraction at a price.
"""

from __future__ import annotations

from dataclasses import dataclass

MARKET_CN = "CN"
MARKET_HK = "HK"

MARKETS = (MARKET_CN, MARKET_HK)

# Reference book used to express the per-trade MINIMUM commission as bps. The
# model is percentage-based while a broker minimum is an absolute fee, so we
# pin it to a documented capital × the smallest standard sleeve. At ¥1M / 10%
# (¥100k notional): CN ¥5 -> 0.5bp (< 3bp) and HK$100 -> 10bp (< 20bp), so the
# minimum does NOT bind for the default book; it only bites for smaller
# accounts / smaller sleeves. Keeping it explicit documents the residual.
REFERENCE_CAPITAL = 1_000_000.0
REFERENCE_SLEEVE_FRAC = 0.10

CN_TICK = 0.01

# HKEX minimum spread table: (exclusive upper price bound, tick).
_HK_TICK_TABLE: tuple[tuple[float, float], ...] = (
    (0.25, 0.001),
    (0.50, 0.005),
    (10.0, 0.010),
    (20.0, 0.020),
    (100.0, 0.050),
    (200.0, 0.100),
    (500.0, 0.200),
    (1000.0, 0.500),
    (2000.0, 1.000),
    (5000.0, 2.000),
    (float("inf"), 5.000),
)


@dataclass(frozen=True)
class CostParams:
    """Per-market round-trip cost assumptions (all values in basis points)."""

    commission_bps_entry: float
    commission_bps_exit: float
    stamp_bps_entry: float
    stamp_bps_exit: float
    transfer_bps_entry: float
    transfer_bps_exit: float
    levy_bps_entry: float
    levy_bps_exit: float
    slippage_bps_entry: float
    slippage_bps_exit: float
    min_commission_abs: float


# Strict realistic defaults (2026-09-21 recalibration):
# CN: 万3 commission (retail default top tier) + 0.001% transfer + 经手费/证管费
#     + 0.05% sell stamp + 10bps/side slippage floor.
# HK: Ping An online 0.20% commission (highest schedule rate, verified
#     2026-09-08) + 0.1% stamp both sides + ~1.27bps/side micro-levies (SFC +
#     HKEX + AFRC + CCASS) + 15bps/side slippage floor. HK$100/trade minimum
#     and phone-trade 0.25% are not modelled (see REFERENCE_* above).
_COST_PARAMS: dict[str, CostParams] = {
    MARKET_CN: CostParams(
        commission_bps_entry=3.0,
        commission_bps_exit=3.0,
        stamp_bps_entry=0.0,
        stamp_bps_exit=5.0,
        transfer_bps_entry=0.1,
        transfer_bps_exit=0.1,
        levy_bps_entry=0.541,
        levy_bps_exit=0.541,
        slippage_bps_entry=10.0,
        slippage_bps_exit=10.0,
        min_commission_abs=5.0,
    ),
    MARKET_HK: CostParams(
        commission_bps_entry=20.0,
        commission_bps_exit=20.0,
        stamp_bps_entry=10.0,
        stamp_bps_exit=10.0,
        transfer_bps_entry=0.0,
        transfer_bps_exit=0.0,
        levy_bps_entry=1.27,
        levy_bps_exit=1.27,
        slippage_bps_entry=15.0,
        slippage_bps_exit=15.0,
        min_commission_abs=100.0,
    ),
}


def markets() -> tuple[str, ...]:
    """Supported paper-trade markets (order is stable)."""
    return MARKETS


def _params(market: str) -> CostParams:
    if market not in _COST_PARAMS:
        raise ValueError(f"no cost model for market {market!r} (known: {sorted(_COST_PARAMS)})")
    return _COST_PARAMS[market]


def tick_size_for(market: str, price: float | None) -> float:
    """Minimum price increment (local currency) for a market at a price."""
    if price is None or price <= 0:
        return 0.0
    if market == MARKET_CN:
        return CN_TICK
    if market == MARKET_HK:
        for upper, tick in _HK_TICK_TABLE:
            if price < upper:
                return tick
        return _HK_TICK_TABLE[-1][1]
    return 0.0


def _effective_commission_bps(market: str, *, entry: bool) -> float:
    p = _params(market)
    base = p.commission_bps_entry if entry else p.commission_bps_exit
    ref_notional = REFERENCE_CAPITAL * REFERENCE_SLEEVE_FRAC
    min_bps = p.min_commission_abs / ref_notional * 10000.0 if ref_notional > 0 else 0.0
    return max(base, min_bps)


def explicit_entry_frac(market: str) -> float:
    """Entry-side explicit fees (commission + stamp + transfer + levies)."""
    p = _params(market)
    bps = (
        _effective_commission_bps(market, entry=True)
        + p.stamp_bps_entry
        + p.transfer_bps_entry
        + p.levy_bps_entry
    )
    return bps / 10000.0


def explicit_exit_frac(market: str) -> float:
    """Exit-side explicit fees (commission + stamp + transfer + levies)."""
    p = _params(market)
    bps = (
        _effective_commission_bps(market, entry=False)
        + p.stamp_bps_exit
        + p.transfer_bps_exit
        + p.levy_bps_exit
    )
    return bps / 10000.0


def explicit_round_trip_cost_pct(market: str) -> float:
    """Round-trip explicit fees only, explicit fees as a fraction (no slippage)."""
    return explicit_entry_frac(market) + explicit_exit_frac(market)


def slippage_frac(market: str, price: float | None = None) -> float:
    """One-side slippage as a fraction: ``max(base_bps, tick/price)``.

    With no ``price`` the base bps floor is returned (static). A valid price
    raises the floor to one tick so low-priced names are not under-charged.
    """
    p = _params(market)
    base = p.slippage_bps_entry / 10000.0
    if price is None or price <= 0:
        return base
    tick = tick_size_for(market, price)
    if tick <= 0:
        return base
    return max(base, tick / float(price))


def round_trip_cost_pct(market: str, price: float | None = None) -> float:
    """Round-trip cost as a fraction of position size (0.0032 == 0.32%).

    Used at close time: net_pnl_pct = gross_pnl_pct - round_trip_cost_pct*100.
    With ``price`` the one-tick slippage floor applies; without it the static
    base floor is used. Unknown markets raise ValueError.
    """
    return explicit_round_trip_cost_pct(market) + 2.0 * slippage_frac(market, price)


def round_trip_cost_pct_at(
    market: str, entry_price: float | None, exit_price: float | None
) -> float:
    """Round-trip cost fraction using the actual entry/exit prices for slippage."""
    return (
        explicit_round_trip_cost_pct(market)
        + slippage_frac(market, entry_price)
        + slippage_frac(market, exit_price)
    )


def net_pnl_pct(gross_pnl_pct: float, market: str, price: float | None = None) -> float:
    """Net pnl % for a close: gross minus the market's round-trip cost."""
    return gross_pnl_pct - round_trip_cost_pct(market, price) * 100.0


def entry_cost_frac(market: str, price: float | None = None) -> float:
    """Entry-side cost as a fraction of sleeve size (fees + slippage)."""
    return explicit_entry_frac(market) + slippage_frac(market, price)


def exit_cost_frac(market: str, price: float | None = None) -> float:
    """Exit-side cost as a fraction of sleeve size (fees + slippage)."""
    return explicit_exit_frac(market) + slippage_frac(market, price)
