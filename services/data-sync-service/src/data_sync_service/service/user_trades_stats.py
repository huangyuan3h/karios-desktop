"""Expectancy / win-rate stats for user-entered trades (SELL legs).

The expectancy board answers "is this disciplined system net positive?":

    expectancy = win_rate × avg_win − loss_rate × avg_loss − round_trip_cost

- ``win_rate``  = wins / closed trades (pnl_pct > 0)
- ``avg_win``   = mean of positive pnl_pct
- ``avg_loss``  = mean of negative pnl_pct (positive number)
- ``profit_factor`` = sum(wins) / |sum(losses)|  (inf if no losses, None if no wins)
- ``avg_holding_days`` = mean holding_days over closed trades
- ``net_expectancy_pct`` = expectancy − round_trip_cost (default 0.3)

Also breaks down per source (TV / ALPHA / MANUAL / other) so the user can see
which entry channel actually pays for itself (feeds TIP-011 attribution).
"""

from __future__ import annotations

from statistics import fmean
from typing import Any

from data_sync_service.db.user_trades import fetch_sell_rows

ROUND_TRIP_COST_PCT = 0.3


def _bucket_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "count": 0,
            "wins": 0,
            "losses": 0,
            "winRate": None,
            "avgWinPct": None,
            "avgLossPct": None,
            "expectancyPct": None,
            "netExpectancyPct": None,
            "profitFactor": None,
            "avgHoldingDays": None,
        }
    pnls = [float(r["pnlPct"]) for r in rows if r.get("pnlPct") is not None]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    win_rate = len(wins) / len(pnls) if pnls else None
    avg_win = fmean(wins) if wins else None
    avg_loss = fmean(losses) if losses else None  # negative number
    gross = (
        win_rate * avg_win - (1 - win_rate) * abs(avg_loss)
        if win_rate is not None and avg_win is not None and avg_loss is not None
        else None
    )
    profit_factor = (
        sum(wins) / abs(sum(losses)) if sum(losses) != 0 else (None if not wins else float("inf"))
    )
    holding = [int(r["holdingDays"]) for r in rows if r.get("holdingDays") is not None]
    return {
        "count": len(rows),
        "wins": len(wins),
        "losses": len(losses),
        "winRate": round(win_rate, 4) if win_rate is not None else None,
        "avgWinPct": round(avg_win, 3) if avg_win is not None else None,
        "avgLossPct": round(abs(avg_loss), 3) if avg_loss is not None else None,
        "expectancyPct": round(gross, 3) if gross is not None else None,
        "netExpectancyPct": round(gross - ROUND_TRIP_COST_PCT, 3)
        if gross is not None
        else None,
        "profitFactor": round(profit_factor, 3) if profit_factor is not None else None,
        "avgHoldingDays": round(fmean(holding), 1) if holding else None,
    }


def compute_trade_stats() -> dict[str, Any]:
    rows = fetch_sell_rows()
    stats = _bucket_stats(rows)
    by_source: dict[str, dict[str, Any]] = {}
    by_symbol: dict[str, dict[str, Any]] = {}
    for r in rows:
        source = r.get("source") or "UNKNOWN"
        sym = r["symbol"]
        if source not in by_source:
            by_source[source] = []
        if sym not in by_symbol:
            by_symbol[sym] = []
        by_source[source].append(r)
        by_symbol[sym].append(r)
    stats["bySource"] = {
        source: _bucket_stats(rows) for source, rows in sorted(by_source.items())
    }
    stats["bySymbol"] = {
        sym: _bucket_stats(rows) for sym, rows in sorted(by_symbol.items())
    }
    stats["roundTripCostPct"] = ROUND_TRIP_COST_PCT
    stats["total"] = len(rows)
    return stats
