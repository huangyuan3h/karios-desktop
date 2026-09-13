"""Harbor parking automation for the paper book (idle-cash ETF sleeve).

Daily close job: evaluate the Harbor sleeve state machine against the PAPER
book (build_multi_asset_sleeve) and mirror the decision into paper_trades:

  BUY             -> open the ETF pick with sleeve_pct = idle% (T+1 open fill)
  ROTATE          -> close the old leg, open the new pick (T+1 open fill)
  SELL_TO_REPO    -> close the open sleeve leg (no candidate / trail8)
  HOLD / DONT_BUY -> no-op

Idempotent: insert_paper_trade has ON CONFLICT (symbol, entry_date, side);
close_paper_trade only touches open rows. Rule: B11
docs/backtests/stable/etf-parking-baseline-2026-09-13.md.
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
from data_sync_service.service.multi_asset_sleeve import (  # noqa: E402
    CANDIDATES,
    build_multi_asset_sleeve,
)
from data_sync_service.service.paper_entry_fill import resolve_next_open_fill  # noqa: E402
from data_sync_service.service.portfolio_health import _health_block  # noqa: E402

CANDIDATE_SYMBOLS = {c["symbol"] for c in CANDIDATES}


def _open_sleeve_legs() -> list[dict[str, Any]]:
    return [
        t
        for t in list_paper_trades(status="open")
        if str(t.get("symbol") or "").upper() in CANDIDATE_SYMBOLS
    ]


def _pnl_for(leg: dict[str, Any], close_price: float, day: str) -> tuple[float, int]:
    entry = float(leg.get("entryPrice") or leg.get("entry_price") or 0)
    if entry <= 0 or close_price <= 0:
        return 0.0, 0
    pnl = (close_price / entry - 1.0) * 100.0
    entry_date = str(leg.get("entryDate") or leg.get("entry_date") or "")
    try:
        days = (date.fromisoformat(day) - date.fromisoformat(entry_date[:10])).days
    except (TypeError, ValueError):
        days = 0
    return pnl, max(0, days)


def _exit_fill(leg: dict[str, Any], day: str) -> tuple[float, str] | None:
    """Next-open exit fill for a held ETF leg (Harbor: signal T close -> T+1 open)."""
    sym = str(leg.get("symbol") or "")
    ts = str(leg.get("tsCode") or leg.get("ts_code") or sym.replace("ETF:", "") + ".SH")
    if not ts:
        return None
    fill = resolve_next_open_fill(ts, day)
    if fill is None:
        return None
    px = float(fill.get("entry_price") or 0)
    if px <= 0:
        return None
    return px, str(fill.get("entry_date") or day)


def _build_multi_for_paper(day: str) -> dict[str, Any]:
    """Multi-asset sleeve evaluated against the PAPER book (Nasdaq-first)."""
    cn_block = _health_block(market="CN", day=day)
    open_trades = list_paper_trades(status="open")
    holdings = [
        {
            "symbol": t.get("symbol"),
            "ts_code": t.get("ts_code"),
            "sleeve_pct": t.get("sleeve_pct") or 0,
        }
        for t in open_trades
        if str(t.get("symbol") or "").upper().startswith(("CN:", "ETF:"))
    ]
    return build_multi_asset_sleeve(day=day, cn_block=cn_block, holdings_override=holdings)


def apply_sleeve_to_paper(*, day: str) -> dict[str, Any]:
    """Run the multi-asset sleeve (GOLD/OIL/NASDAQ/BOND 60/200+5d) against the paper book.

    Single-asset T6 fallback removed 2026-08-24 (multi validated tri-window).
    Entry fill uses next_open (same as S-3 paper / backtest realism).
    """
    multi = _build_multi_for_paper(day)
    action = multi.get("action")
    pick = multi.get("pick") or {}
    price = pick.get("close")
    idle = float(multi.get("idlePct") or 0.0)
    open_multi = _open_sleeve_legs()
    ts = str(pick.get("ts") or pick.get("ts_code") or "")
    if not ts and pick.get("symbol"):
        bare = str(pick.get("symbol")).replace("ETF:", "")
        ts = bare if "." in bare else f"{bare}.SH"

    def _fill_or_none() -> dict[str, Any] | None:
        if not ts:
            return None
        return resolve_next_open_fill(ts, day, signal_close=float(price) if price else None)

    if action == "BUY" and not open_multi:
        fill = _fill_or_none()
        if fill is None:
            return {"day": day, "action": action, "changed": False, "reason": "no next_open fill"}
        row = insert_paper_trade(
            symbol=pick.get("symbol") or "ETF:513350",
            entry_date=str(fill["entry_date"]),
            side="BUY",
            entry_price=float(fill["entry_price"]),
            why_at_entry=f"multi-sleeve: {pick.get('key')} mom60 {pick.get('mom60')}% 60/200+5d",
            sleeve_pct=idle,
            source=SOURCE_S3,
            market="CN",
            signal_snapshot=fill.get("signal_snapshot"),
        )
        return {
            "day": day,
            "action": action,
            "changed": bool(row),
            "reason": "multi opened",
            "price": fill["entry_price"],
            "entryDate": fill["entry_date"],
            "symbol": pick.get("symbol"),
        }
    if action == "ROTATE" and open_multi:
        fill = _fill_or_none()
        if fill is None:
            return {"day": day, "action": action, "changed": False, "reason": "no next_open fill"}
        for leg in open_multi:
            exit_fill = _exit_fill(leg, day)
            if exit_fill is None:
                continue
            px, exit_day = exit_fill
            pnl, days = _pnl_for(leg, px, exit_day)
            close_paper_trade(
                trade_id=str(leg.get("id")),
                close_date=exit_day,
                close_price=px,
                pnl_pct=pnl,
                holding_days=days,
                close_reason=CLOSE_REASON_SLEEVE_EXIT,
            )
        row = insert_paper_trade(
            symbol=pick.get("symbol") or "ETF:513350",
            entry_date=str(fill["entry_date"]),
            side="BUY",
            entry_price=float(fill["entry_price"]),
            why_at_entry=f"harbor parking rotate to {pick.get('key')}",
            sleeve_pct=idle,
            source=SOURCE_S3,
            market="CN",
            signal_snapshot=fill.get("signal_snapshot"),
        )
        return {
            "day": day,
            "action": action,
            "changed": True,
            "reason": "rotated",
            "price": fill["entry_price"],
            "entryDate": fill["entry_date"],
        }
    if action == "SELL_TO_REPO" and open_multi:
        closed = 0
        for leg in open_multi:
            exit_fill = _exit_fill(leg, day)
            if exit_fill is None:
                continue
            px, exit_day = exit_fill
            pnl, days = _pnl_for(leg, px, exit_day)
            if close_paper_trade(
                trade_id=str(leg.get("id")),
                close_date=exit_day,
                close_price=px,
                pnl_pct=pnl,
                holding_days=days,
                close_reason=CLOSE_REASON_SLEEVE_EXIT,
            ):
                closed += 1
        return {
            "day": day,
            "action": action,
            "changed": closed > 0,
            "reason": f"multi closed {closed}",
            "price": price,
        }
    return {"day": day, "action": action or "NONE", "changed": False, "reason": "multi no-op"}
