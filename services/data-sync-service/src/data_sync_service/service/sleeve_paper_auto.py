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
from data_sync_service.service.multi_asset_sleeve import COST as SLEEVE_COST  # noqa: E402
from data_sync_service.service.paper_entry_fill import resolve_next_open_fill  # noqa: E402
from data_sync_service.service.portfolio_health import _health_block  # noqa: E402

CANDIDATE_SYMBOLS = {c["symbol"] for c in CANDIDATES}
SLEEVE_COST_PCT = round(SLEEVE_COST * 2.0 * 100.0, 4)


def normalize_sleeve_pct(value: Any) -> float:
    """Paper-book size in percent.

    S-3 paper rows store fractions (0.10); Harbor sleeve rows store percent
    (idle 0-100); legacy/backfilled rows may carry either. Values below 1.0
    are treated as fractions.
    """
    try:
        v = float(value)
    except (TypeError, ValueError):
        return 0.0
    if 0.0 < v < 1.0:
        v *= 100.0
    return v


def paper_sleeve_holdings(rows: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    """Open CN:/ETF: paper rows in the shape ``build_multi_asset_sleeve`` expects.

    Reads the db camelCase contract (``sleevePct`` / ``entryDate`` / ``tsCode``)
    and normalizes sizes to percent. ``entryDate`` is required by the causal
    trail8 exit — without it the Live state machine silently skips it.
    """
    if rows is None:
        rows = list_paper_trades(status="open")
    out: list[dict[str, Any]] = []
    for t in rows:
        sym = str(t.get("symbol") or "")
        if not sym.upper().startswith(("CN:", "ETF:")):
            continue
        sleeve = t.get("sleevePct")
        if sleeve is None:
            sleeve = t.get("sleeve_pct")
        out.append(
            {
                "symbol": sym,
                "ts_code": t.get("tsCode") or t.get("ts_code"),
                "entryDate": t.get("entryDate") or t.get("entry_date"),
                "sleeve_pct": normalize_sleeve_pct(sleeve),
            }
        )
    return out


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


def _close_leg(
    leg: dict[str, Any],
    close_price: float,
    exit_day: str,
    *,
    snapshot_extra: dict[str, Any] | None = None,
) -> bool:
    """Close one ETF leg booking gross/net/costs like the S-3 paper path."""
    pnl, days = _pnl_for(leg, close_price, exit_day)
    return bool(
        close_paper_trade(
            trade_id=str(leg.get("id")),
            close_date=exit_day,
            close_price=close_price,
            pnl_pct=pnl - SLEEVE_COST_PCT,
            holding_days=days,
            close_reason=CLOSE_REASON_SLEEVE_EXIT,
            gross_pnl_pct=pnl,
            costs_pct=SLEEVE_COST_PCT,
            signal_snapshot_extra=snapshot_extra,
        )
    )


def _exit_fill(leg: dict[str, Any], day: str) -> tuple[float, str, dict[str, Any]] | None:
    """Next-open exit fill for a held ETF leg (Harbor: signal T close -> T+1 open).

    Same-evening (18:20) the T+1 open does not exist yet: `resolve_next_open_fill`
    returns the signal-day close as a placeholder, so the exit is marked
    ``exitPendingOpenFill`` and the real open is patched by `run_update` once the
    bar lands (mirrors the entry `pendingOpenFill` flow).
    """
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
    extra: dict[str, Any] = {"exitSignalDate": day}
    if fill.get("pending_open_fill"):
        extra["exitPendingOpenFill"] = True
        extra["exitPlaceholderClose"] = px
    return px, str(fill.get("entry_date") or day), extra


def _build_multi_for_paper(day: str) -> dict[str, Any]:
    """Multi-asset sleeve evaluated against the PAPER book."""
    cn_block = _health_block(market="CN", day=day)
    holdings = paper_sleeve_holdings()
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
    size = multi.get("parkPct")
    if size is None:
        size = multi.get("idlePct")
    idle = float(size or 0.0)
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
        exits: list[tuple[dict[str, Any], float, str, dict[str, Any]]] = []
        for leg in open_multi:
            exit_fill = _exit_fill(leg, day)
            if exit_fill is None:
                return {
                    "day": day,
                    "action": action,
                    "changed": False,
                    "reason": "rotate aborted: no exit fill for open leg",
                }
            exits.append((leg, exit_fill[0], exit_fill[1], exit_fill[2]))
        closed = sum(
            1
            for leg, px, exit_day, extra in exits
            if _close_leg(leg, px, exit_day, snapshot_extra=extra)
        )
        if closed != len(exits):
            return {
                "day": day,
                "action": action,
                "changed": closed > 0,
                "reason": f"rotate aborted: closed {closed}/{len(exits)}",
            }
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
            px, exit_day, extra = exit_fill
            if _close_leg(leg, px, exit_day, snapshot_extra=extra):
                closed += 1
        return {
            "day": day,
            "action": action,
            "changed": closed > 0,
            "reason": f"multi closed {closed}",
            "price": price,
        }
    return {"day": day, "action": action or "NONE", "changed": False, "reason": "multi no-op"}
