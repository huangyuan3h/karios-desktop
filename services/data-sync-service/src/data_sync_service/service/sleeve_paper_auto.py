"""Sleeve auto-configuration for the paper book (T6 · 2026-08-21 落地).

Daily close job: evaluate the sleeve state machine against the PAPER book
(build_third_asset_sleeve_for_paper) and mirror the decision into
paper_trades:

  BUY_513100      -> open ETF:513100 with sleeve_pct = idle%
  SELL_TO_REPO    -> close the open sleeve leg (broke MA200)
  SELL_TO_A_SHARE -> close the open sleeve leg (A-share buy points)
  HOLD / DONT_BUY -> no-op

Idempotent: insert_paper_trade has ON CONFLICT (symbol, entry_date, side);
close_paper_trade only touches open rows. The three-window validation of the
underlying rule lives in scripts/sleeve_nav_sim.py (all-windows positive
delta, OPT-119).
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

logger = logging.getLogger(__name__)

from data_sync_service.db.paper_trading import (  # noqa: E402
    CLOSE_REASON_SLEEVE_EXIT,
    SOURCE_S3,
    close_paper_trade,
    insert_paper_trade,
    list_paper_trades,
)
from data_sync_service.service.third_asset_sleeve import (  # noqa: E402
    ACTION_BUY,
    ACTION_SELL_TO_A_SHARE,
    ACTION_SELL_TO_REPO,
    THIRD_ASSET_SYMBOL,
    build_third_asset_sleeve_for_paper,
)


def _open_sleeve_legs() -> list[dict[str, Any]]:
    return [
        t for t in list_paper_trades(status="open")
        if str(t.get("symbol") or "").upper() == THIRD_ASSET_SYMBOL
    ]


def _pnl_for(leg: dict[str, Any], close_price: float, day: str) -> tuple[float, int]:
    entry = float(leg.get("entry_price") or 0)
    if entry <= 0:
        return 0.0, 0
    pnl = (close_price / entry - 1.0) * 100.0
    try:
        days = (date.fromisoformat(day) - date.fromisoformat(str(leg.get("entry_date")))).days
    except (TypeError, ValueError):
        days = 0
    return pnl, max(0, days)


def apply_sleeve_to_paper(*, day: str) -> dict[str, Any]:
    """Run the sleeve decision against the paper book for ``day``.

    Returns the action taken + what changed. Safe to run repeatedly.
    """
    sleeve = build_third_asset_sleeve_for_paper(day=day)
    action = sleeve.get("action")
    price = sleeve.get("price")
    idle = float(sleeve.get("idlePct") or 0.0)
    open_legs = _open_sleeve_legs()

    if action == ACTION_BUY and not open_legs:
        if price is None:
            return {"day": day, "action": action, "changed": False, "reason": "no price"}
        row = insert_paper_trade(
            symbol=THIRD_ASSET_SYMBOL,
            entry_date=day,
            side="BUY",
            entry_price=float(price),
            why_at_entry=f"sleeve: idle {idle:.0f}% & above MA200 (T6)",
            sleeve_pct=idle,
            source=SOURCE_S3,
            market="CN",
        )
        return {
            "day": day, "action": action, "changed": bool(row), "reason": "opened",
            "price": price, "sleevePct": round(idle, 1),
        }

    if action in (ACTION_SELL_TO_REPO, ACTION_SELL_TO_A_SHARE) and open_legs:
        closed = 0
        for leg in open_legs:
            pnl, days = _pnl_for(leg, float(price or 0), day)
            updated = close_paper_trade(
                trade_id=str(leg.get("id")),
                close_date=day,
                close_price=float(price or 0),
                pnl_pct=pnl,
                holding_days=days,
                close_reason=CLOSE_REASON_SLEEVE_EXIT,
            )
            if updated:
                closed += 1
        return {
            "day": day, "action": action, "changed": closed > 0, "reason": f"closed {closed}",
            "price": price,
        }

    return {"day": day, "action": action or "NONE", "changed": False, "reason": "no-op"}