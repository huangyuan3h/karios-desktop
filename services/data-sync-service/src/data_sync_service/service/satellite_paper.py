"""Satellite forward paper book (OPT-228 / OPT-186 slice 2a, read-only).

The watchlist satellite card shows today's 14:30 decision (OPT-222), but not
how the strategy is actually doing since the five-strategy freeze — nor the
``paper 20 笔`` go-live prerequisite. This module projects a **forward paper
book** from the frozen habit replay::

    build_sgap_timeline(start=INCEPTION, end=today, **HABIT_RECIPE)

Because the habit recipe is fully frozen, replaying from inception reproduces
exactly what forward paper logging would have recorded for the same window —
zero drift with the timeline/backtest (single engine). Nothing is persisted.

Scope (slice 2a): read-only book + stats. Writing ``paper_trades`` and the
``user_trades`` satellite leg + reconciliation are slice 2b (they touch the
shared paper ledger / DB constraints and are deliberately out of scope here).

Pure projection seam: ``satellite_paper_from_sat`` takes the replay output so
the shaping is unit-testable without Postgres.
"""

from __future__ import annotations

import logging
import time
from typing import Any

logger = logging.getLogger(__name__)

# The forward book changes at most once per session but the build is heavy
# (5-min context + replay, ~30s). Memoize per (start, end) so a page reload or
# a second satellite-mode mount does not re-run the replay.
_CACHE: dict[tuple[str, str], tuple[float, dict[str, Any]]] = {}
CACHE_TTL_SECONDS = 15 * 60

# First session after the five-strategy freeze (2026-09-18 is the H2 unification
# day). The forward book only counts trades entered on/after this date, so the
# ``paper 20 笔`` counter grows with real time instead of replaying history.
SATELLITE_PAPER_INCEPTION = "2026-09-18"
# Go-live prerequisite from `docs/modules/strategy-recipes.md` §5.
PREREQ_TRADES = 20


def _net_pnl_pct(gross_pct: float | None, cost_pct: float) -> float | None:
    """Per-trade net return: gross minus the round-trip cost in pct points."""
    if gross_pct is None:
        return None
    return round(float(gross_pct) - cost_pct, 2)


def satellite_paper_from_sat(sat: dict[str, Any], *, start: str, end: str) -> dict[str, Any]:
    """Shape a replay output into the forward paper book (pure)."""
    from data_sync_service.service.state_bucket_track import COSTS_ROUNDTRIP

    cost_pct = float(COSTS_ROUNDTRIP) * 100.0  # fraction -> pct points (0.003 -> 0.3)
    blotter = sat.get("blotter") or []
    closed: list[dict[str, Any]] = []
    by_reason: dict[str, int] = {}
    for row in blotter:
        if row.get("kind") != "fill":
            continue
        entry_date = row.get("entryDate")
        if not entry_date or str(entry_date) < start:
            continue
        gross = row.get("pnlPct")
        net = _net_pnl_pct(gross, cost_pct)
        reason = str(row.get("closeReason") or "unknown")
        by_reason[reason] = by_reason.get(reason, 0) + 1
        closed.append(
            {
                "ts": row.get("ts"),
                "entryDate": entry_date,
                "exitDate": row.get("exitDate"),
                "entryPxSrc": row.get("entryPxSrc"),
                "exitPxSrc": row.get("exitPxSrc"),
                "grossPnlPct": None if gross is None else round(float(gross), 2),
                "netPnlPct": net,
                "heldDays": row.get("heldDays"),
                "closeReason": reason,
                "ampPct": row.get("amp"),
                "ampRank": row.get("ampRank"),
            }
        )
    # Newest first is what an operations card wants.
    closed.sort(key=lambda r: (str(r.get("exitDate") or ""), str(r.get("entryDate") or "")), reverse=True)

    open_legs: list[dict[str, Any]] = []
    for p in sat.get("openPositions") or []:
        entry_date = p.get("entryDate")
        if not entry_date or str(entry_date) < start:
            continue
        open_legs.append(
            {
                "ts": p.get("ts"),
                "entryDate": entry_date,
                "entryPrice": p.get("entryPrice"),
                "close": p.get("close"),
                "heldDays": p.get("heldDays"),
                "daysLeft": p.get("daysLeft"),
                "exitDue": p.get("exitDue"),
                "pnlPct": p.get("pnlPct"),
            }
        )

    nets = [r["netPnlPct"] for r in closed if r["netPnlPct"] is not None]
    wins = [n for n in nets if n > 0]
    summary = sat.get("summary") or {}
    closed_count = len(closed)
    return {
        "ok": True,
        "start": start,
        "end": end,
        "inception": start,
        "prereq": {
            "closedCount": closed_count,
            "target": PREREQ_TRADES,
            "met": closed_count >= PREREQ_TRADES,
        },
        "stats": {
            "closedCount": closed_count,
            "openCount": len(open_legs),
            "winCount": len(wins),
            "winRate": round(len(wins) / closed_count, 3) if closed_count else None,
            "avgNetPnlPct": round(sum(nets) / len(nets), 2) if nets else None,
            "bestNetPnlPct": max(nets) if nets else None,
            "worstNetPnlPct": min(nets) if nets else None,
            "paperPct": summary.get("satPct"),
            "paperMaxDdPct": summary.get("satMaxDdPct"),
            "avgHeldDays": summary.get("avgHeldDays"),
            "closeReasons": by_reason,
        },
        "closed": closed,
        "openLegs": open_legs,
    }


def build_satellite_paper(
    end: str | None = None, *, start: str | None = None
) -> dict[str, Any]:
    """Forward paper book for [start, end] (defaults: inception -> Shanghai today).

    Heavy read (loads the 5-min context with 120-day warmup); the API is
    read-only and the caller may cache. A replay failure returns an empty book
    with ``decisionAvailable: false`` instead of raising, so the card degrades.
    """
    from data_sync_service.service.state_bucket_track import (
        HABIT_CTX_TIMES,
        HABIT_RECIPE,
        build_sgap_timeline,
    )
    from data_sync_service.service.trade_calendar_utils import shanghai_today_iso

    start = start or SATELLITE_PAPER_INCEPTION
    end = end or shanghai_today_iso()
    cache_key = (start, end)
    hit = _CACHE.get(cache_key)
    if hit is not None and time.time() - hit[0] < CACHE_TTL_SECONDS:
        return hit[1]
    if end < start:
        return {
            "ok": True,
            "start": start,
            "end": end,
            "inception": start,
            "decisionAvailable": False,
            "reason": "end before inception",
            "prereq": {"closedCount": 0, "target": PREREQ_TRADES, "met": False},
            "stats": {
                "closedCount": 0,
                "openCount": 0,
                "winCount": 0,
                "winRate": None,
                "avgNetPnlPct": None,
                "bestNetPnlPct": None,
                "worstNetPnlPct": None,
                "paperPct": None,
                "paperMaxDdPct": None,
                "avgHeldDays": None,
                "closeReasons": {},
            },
            "closed": [],
            "openLegs": [],
        }
    try:
        sat = build_sgap_timeline(start=start, end=end, times=HABIT_CTX_TIMES, **HABIT_RECIPE)
    except Exception as exc:  # noqa: BLE001
        logger.warning("satellite paper build failed: %s", exc)
        return {
            "ok": True,
            "start": start,
            "end": end,
            "inception": start,
            "decisionAvailable": False,
            "reason": f"replay failed: {exc}",
            "prereq": {"closedCount": 0, "target": PREREQ_TRADES, "met": False},
            "stats": {
                "closedCount": 0,
                "openCount": 0,
                "winCount": 0,
                "winRate": None,
                "avgNetPnlPct": None,
                "bestNetPnlPct": None,
                "worstNetPnlPct": None,
                "paperPct": None,
                "paperMaxDdPct": None,
                "avgHeldDays": None,
                "closeReasons": {},
            },
            "closed": [],
            "openLegs": [],
        }
    out = satellite_paper_from_sat(sat, start=start, end=end)
    out["decisionAvailable"] = True
    _CACHE[cache_key] = (time.time(), out)
    return out


# ---------------------------------------------------------------------------
# User book (2026-09-23): the same forward paper, but sourced from the user's
# REAL journal (`user_trades` leg='satellite') instead of the frozen replay.
# The replay book stays as the engine's comparison; this one is what the user
# actually did, so the ``paper 20 笔`` prerequisite counts realised trades.
# ---------------------------------------------------------------------------


def satellite_user_book_from_rows(
    rows: list[dict[str, Any]],
    *,
    start: str,
    end: str,
    cost_pct: float | None = None,
    strategy_mode: str = "starship_b",
) -> dict[str, Any]:
    """Shape the user's real satellite journal into a forward book (pure).

    FIFO-matches BUY/ADD lots to SELL lots per symbol (a partial sell closes
    only the matching share). A trade counts toward the prerequisite only once
    it is CLOSED — an open leg is listed but not counted, same rule as the
    replay book.
    """
    from data_sync_service.service.state_bucket_track import COSTS_ROUNDTRIP

    cost = float(COSTS_ROUNDTRIP) * 100.0 if cost_pct is None else float(cost_pct)
    bounded_rows = [r for r in rows if str(r.get("tradeDate") or "") <= end]
    ordered = sorted(
        bounded_rows,
        key=lambda x: (str(x.get("tradeDate") or ""), str(x.get("createdAt") or "")),
    )
    open_lots: dict[str, list[dict[str, Any]]] = {}
    closed: list[dict[str, Any]] = []
    for r in ordered:
        sym = str(r.get("symbol") or "")
        side = str(r.get("side") or "").upper()
        try:
            px = float(r.get("price") or 0.0)
            pct = float(r.get("positionPct") or 0.0)
        except (TypeError, ValueError):
            continue
        day = str(r.get("tradeDate") or "")
        if not sym or px <= 0 or pct <= 0:
            continue
        if side in ("BUY", "ADD"):
            open_lots.setdefault(sym, []).append(
                {"entryDate": day, "entryPrice": px, "pct": pct}
            )
            continue
        if side != "SELL":
            continue
        remaining = pct
        lots = open_lots.get(sym) or []
        while remaining > 1e-9 and lots:
            lot = lots[0]
            take = min(remaining, float(lot["pct"]))
            gross = (px / float(lot["entryPrice"]) - 1.0) * 100.0
            closed.append(
                {
                    "ts": sym,
                    "entryDate": lot["entryDate"],
                    "exitDate": day,
                    "entryPrice": round(float(lot["entryPrice"]), 4),
                    "exitPrice": round(px, 4),
                    "positionPct": round(take, 4),
                    "grossPnlPct": round(gross, 2),
                    "netPnlPct": round(gross - cost, 2),
                    "heldDays": None,
                    "entryPxSrc": "journal",
                    "exitPxSrc": "journal",
                    "closeReason": "journal",
                }
            )
            lot["pct"] = float(lot["pct"]) - take
            remaining -= take
            if float(lot["pct"]) <= 1e-9:
                lots.pop(0)

    open_legs = [
        {
            "ts": sym,
            "entryDate": lot["entryDate"],
            "entryPrice": round(float(lot["entryPrice"]), 4),
            "positionPct": round(float(lot["pct"]), 4),
            "close": None,
            "heldDays": None,
            "daysLeft": None,
            "exitDue": None,
            "pnlPct": None,
        }
        for sym, lots in open_lots.items()
        for lot in lots
    ]
    # Same window rule as the replay book: only legs entered on/after inception.
    closed = [c for c in closed if str(c["entryDate"]) >= start]
    open_legs = [leg for leg in open_legs if str(leg["entryDate"]) >= start]
    closed.sort(key=lambda r: (str(r["exitDate"]), str(r["entryDate"])), reverse=True)

    nets = [float(c["netPnlPct"]) for c in closed]
    wins = [n for n in nets if n > 0]
    return {
        "ok": True,
        "start": start,
        "end": end,
        "inception": start,
        "source": "user_journal",
        "strategyMode": strategy_mode,
        "prereq": {
            "closedCount": len(closed),
            "target": PREREQ_TRADES,
            "met": len(closed) >= PREREQ_TRADES,
        },
        "stats": {
            "closedCount": len(closed),
            "openCount": len(open_legs),
            "winCount": len(wins),
            "winRate": round(len(wins) / len(nets), 4) if nets else None,
            "avgNetPnlPct": round(sum(nets) / len(nets), 2) if nets else None,
            "bestNetPnlPct": max(nets) if nets else None,
            "worstNetPnlPct": min(nets) if nets else None,
            "paperPct": None,
            "paperMaxDdPct": None,
            "avgHeldDays": None,
            "closeReasons": {"journal": len(closed)} if closed else {},
        },
        "closed": closed,
        "openLegs": open_legs,
    }


def build_user_satellite_book(
    start: str | None = None,
    end: str | None = None,
    *,
    strategy_mode: str = "starship_b",
) -> dict[str, Any]:
    """Read one strategy's real satellite legs from ``user_trades`` (read-only)."""
    from data_sync_service.db import get_connection
    from data_sync_service.service.trade_calendar_utils import shanghai_today_iso

    start = start or SATELLITE_PAPER_INCEPTION
    end = end or shanghai_today_iso()
    try:
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT symbol, side, trade_date, price, position_pct, created_at "
                "FROM user_trades "
                "WHERE leg = %s AND strategy_mode = %s AND trade_date <= %s "
                "ORDER BY trade_date, created_at",
                ("satellite", strategy_mode, end),
            )
            rows = [
                {
                    "symbol": r[0],
                    "side": r[1],
                    "tradeDate": r[2],
                    "price": r[3],
                    "positionPct": r[4],
                    "createdAt": str(r[5]) if r[5] is not None else "",
                }
                for r in cur.fetchall()
            ]
    except Exception as exc:  # noqa: BLE001
        logger.warning("user satellite book read failed: %s", exc)
        return {
            "ok": True,
            "start": start,
            "end": end,
            "inception": start,
            "source": "user_journal",
            "decisionAvailable": False,
            "reason": f"journal read failed: {exc}",
            "prereq": {"closedCount": 0, "target": PREREQ_TRADES, "met": False},
            "stats": {
                "closedCount": 0,
                "openCount": 0,
                "winCount": 0,
                "winRate": None,
                "avgNetPnlPct": None,
                "bestNetPnlPct": None,
                "worstNetPnlPct": None,
                "paperPct": None,
                "paperMaxDdPct": None,
                "avgHeldDays": None,
                "closeReasons": {},
            },
            "closed": [],
            "openLegs": [],
        }
    out = satellite_user_book_from_rows(
        rows,
        start=start,
        end=end,
        strategy_mode=strategy_mode,
    )
    out["decisionAvailable"] = True
    return out
