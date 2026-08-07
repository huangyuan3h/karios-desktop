"""Paper-trading cost model (OPT-062 / L3-P1).

Per-market round-trip cost estimation for the paper-trading book. The model
is intentionally **conservative and simple** — these are default assumptions,
not broker quotes. Every constant is module-level and tunable in one place;
an "operator" who wants a different assumption edits this file and re-runs.

Cost components applied to a round trip (entry + exit):

- **commission**: broker fee per side (bps of notional).
- **stamp tax**: transaction duty. CN pays on SELL only (0.05% since
  2023-08); HK pays on BOTH sides (0.1%).
- **slippage**: adverse price move between signal and fill, per side (bps).

The model has no FX conversion (HK PnL is reported in HKD). FX is an
explicit L3-P3 refinement — the PnL columns are local-currency per market.

Exposed helpers:

- :func:`markets` — supported markets ('CN', 'HK').
- :func:`round_trip_cost_pct` — total round-trip cost as a fraction of the
  position (e.g. 0.0030 for CN). This is the number subtracted from the
  gross pnl % to get the net pnl %.
"""

from __future__ import annotations

from dataclasses import dataclass

MARKET_CN = "CN"
MARKET_HK = "HK"

MARKETS = (MARKET_CN, MARKET_HK)


@dataclass(frozen=True)
class CostParams:
    """Per-market round-trip cost assumptions (all values in basis points)."""

    commission_bps_entry: float
    commission_bps_exit: float
    stamp_bps_entry: float
    stamp_bps_exit: float
    slippage_bps_entry: float
    slippage_bps_exit: float


# Conservative defaults. CN: 万2.5 commission + 0.05% sell stamp + 10bps
# slippage/side. HK: 0.05% commission + 0.1% stamp both sides + 15bps
# slippage/side (wider intraday spreads, no price limit).
_COST_PARAMS: dict[str, CostParams] = {
    MARKET_CN: CostParams(
        commission_bps_entry=2.5,
        commission_bps_exit=2.5,
        stamp_bps_entry=0.0,
        stamp_bps_exit=5.0,
        slippage_bps_entry=10.0,
        slippage_bps_exit=10.0,
    ),
    MARKET_HK: CostParams(
        commission_bps_entry=5.0,
        commission_bps_exit=5.0,
        stamp_bps_entry=10.0,
        stamp_bps_exit=10.0,
        slippage_bps_entry=15.0,
        slippage_bps_exit=15.0,
    ),
}


def markets() -> tuple[str, ...]:
    """Supported paper-trade markets (order is stable)."""
    return MARKETS


def round_trip_cost_pct(market: str) -> float:
    """Round-trip cost as a fraction of position size (0.0030 == 0.30%).

    Used at close time: net_pnl_pct = gross_pnl_pct - round_trip_cost_pct*100.
    Unknown markets raise ValueError — a paper trade for a market without a
    cost model must be rejected at intake, not silently priced at zero.
    """
    if market not in _COST_PARAMS:
        raise ValueError(f"no cost model for market {market!r} (known: {sorted(_COST_PARAMS)})")
    p = _COST_PARAMS[market]
    total_bps = (
        p.commission_bps_entry
        + p.commission_bps_exit
        + p.stamp_bps_entry
        + p.stamp_bps_exit
        + p.slippage_bps_entry
        + p.slippage_bps_exit
    )
    return total_bps / 100.0 / 100.0


def net_pnl_pct(gross_pnl_pct: float, market: str) -> float:
    """Net pnl % for a close: gross minus the market's round-trip cost."""
    return gross_pnl_pct - round_trip_cost_pct(market) * 100.0
