"""clip4 satellite paper book (OPT-131) — independent of S-3 paper.

Occupancy for live Watchlist is the broker book. This module records what the
frozen S-gap recipe would have done: at most 4 slots × 12.5% NAV, body=3
weekday close exit. No protect stop, pyramid, trailing, or 60-day hold.

source='twin_star' rows must not go through paper_s3 / paper_trading.run_update.
"""

from __future__ import annotations

import logging
from typing import Any

from data_sync_service.db.daily import fetch_last_ohlcv_batch
from data_sync_service.db.paper_trading import (
    CLOSE_REASON_BODY_EXIT,
    SOURCE_TWIN_STAR,
    close_paper_trade,
    insert_paper_trade,
    list_paper_trades,
    today_iso,
)
from data_sync_service.service.paper_cost_model import round_trip_cost_pct
from data_sync_service.service.state_bucket_track import BODY, MAX_POS, POSITION_PCT
from data_sync_service.service.twin_star_daily import (
    SAT_SLOT_NAV_PCT,
    build_twin_star_daily_action,
    cn_symbol_from_ts,
    count_weekdays_inclusive,
)

logger = logging.getLogger(__name__)

SLEEVE_PCT = POSITION_PCT * 0.5  # 0.125 of NAV when sat sleeve is 50%


def _open_twin_star() -> list[dict[str, Any]]:
    rows = list_paper_trades(status="open", market="CN", limit=50)
    return [r for r in rows if str(r.get("source") or "") == SOURCE_TWIN_STAR]


def _held_days(entry_date: str | None, as_of: str) -> int:
    if not entry_date:
        return 0
    return count_weekdays_inclusive(str(entry_date), as_of)


def run_intake_twin_star(*, trade_date: str | None = None) -> dict[str, Any]:
    """Insert clip4 sat candidates as paper BUYs. Cap 4. No refill/pyramid."""
    day = trade_date or today_iso()
    summary: dict[str, Any] = {
        "tradeDate": day,
        "candidates": 0,
        "inserted": 0,
        "skipped": 0,
        "skippedReasons": {},
        "openSlots": 0,
    }

    def _skip(reason: str) -> None:
        summary["skipped"] += 1
        reasons = summary["skippedReasons"]
        reasons[reason] = int(reasons.get(reason) or 0) + 1

    try:
        action = build_twin_star_daily_action()
    except Exception as exc:  # noqa: BLE001
        summary["error"] = f"twin_star action failed: {exc}"
        logger.warning("paper_twin_star intake action failed: %s", exc)
        return summary

    sat = action.get("sat") or {}
    if not sat.get("gateOpen"):
        _skip("gate_closed")
        return summary
    candidates = list(sat.get("candidates") or [])
    summary["candidates"] = len(candidates)
    if not candidates:
        _skip("no_candidates")
        return summary

    try:
        opens = _open_twin_star()
    except Exception as exc:  # noqa: BLE001
        summary["error"] = f"list open twin_star failed: {exc}"
        return summary
    held = {str(r.get("symbol") or "") for r in opens}
    free = max(0, MAX_POS - len(opens))
    summary["openSlots"] = len(opens)
    if free <= 0:
        _skip("slots_full")
        return summary

    ts_codes = [str(c.get("ts") or "") for c in candidates if c.get("ts")]
    closes: dict[str, float] = {}
    try:
        bars = fetch_last_ohlcv_batch(ts_codes, days=5)
        for ts, rows in (bars or {}).items():
            if not rows:
                continue
            close = rows[-1][4] if len(rows[-1]) >= 5 else None
            if close is not None:
                closes[str(ts)] = float(close)
    except Exception as exc:  # noqa: BLE001
        logger.warning("paper_twin_star fetch closes failed: %s", exc)

    for cand in candidates:
        if free <= 0:
            _skip("slots_full")
            continue
        ts = str(cand.get("ts") or "")
        if not ts:
            _skip("no_ts")
            continue
        symbol = cn_symbol_from_ts(ts)
        if symbol in held:
            _skip("already_open")
            continue
        px = closes.get(ts)
        if cand.get("close") is not None:
            try:
                px = float(cand["close"])
            except (TypeError, ValueError):
                pass
        if px is None or px <= 0:
            _skip("no_price")
            continue
        try:
            row = insert_paper_trade(
                symbol=symbol,
                entry_date=day,
                side="BUY",
                entry_price=px,
                why_at_entry="twin_star S-gap clip4",
                sleeve_pct=SLEEVE_PCT,
                source=SOURCE_TWIN_STAR,
                market="CN",
                signal_snapshot={
                    "recipe": "clip4",
                    "body": BODY,
                    "slotNavPct": SAT_SLOT_NAV_PCT,
                    "amp": cand.get("amp"),
                    "gapPct": cand.get("gapPct"),
                },
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("paper_twin_star insert failed %s: %s", symbol, exc)
            _skip("insert_failed")
            continue
        if row is None:
            _skip("idempotent")
            continue
        summary["inserted"] += 1
        held.add(symbol)
        free -= 1
    return summary


def run_update_twin_star(*, today_iso_s: str | None = None) -> dict[str, Any]:
    """Close twin_star paper at body=3 close. No protect stop (frozen S-gap)."""
    day = today_iso_s or today_iso()
    summary: dict[str, Any] = {
        "today": day,
        "scanned": 0,
        "closed": 0,
        "closeReasons": {},
    }
    try:
        opens = _open_twin_star()
    except Exception as exc:  # noqa: BLE001
        summary["error"] = f"list open twin_star failed: {exc}"
        return summary
    summary["scanned"] = len(opens)
    if not opens:
        return summary

    ts_codes = []
    for t in opens:
        ts = str((t.get("symbol") or "").replace("CN:", ""))
        if len(ts) == 6:
            suffix = "SH" if ts.startswith("6") else "SZ"
            ts_codes.append(f"{ts}.{suffix}")
    closes: dict[str, float] = {}
    try:
        bars = fetch_last_ohlcv_batch(ts_codes, days=8)
        for ts, rows in (bars or {}).items():
            if rows:
                closes[str(ts)] = float(rows[-1][4])
    except Exception as exc:  # noqa: BLE001
        logger.warning("paper_twin_star update fetch failed: %s", exc)

    costs_pct = round_trip_cost_pct("CN") * 100.0
    for t in opens:
        trade_id = str(t.get("id") or "")
        symbol = str(t.get("symbol") or "")
        ts = None
        if symbol.upper().startswith("CN:") and len(symbol) >= 9:
            ticker = symbol[3:9]
            ts = f"{ticker}.SH" if ticker.startswith("6") else f"{ticker}.SZ"
        px = closes.get(ts or "")
        entry = float(t.get("entryPrice") or t.get("entry_price") or 0)
        if px is None or entry <= 0 or not trade_id:
            continue
        gross = (px - entry) / entry * 100.0
        net = gross - costs_pct
        held = _held_days(str(t.get("entryDate") or t.get("entry_date") or ""), day)
        if held < BODY:
            continue
        reason = CLOSE_REASON_BODY_EXIT
        try:
            closed = close_paper_trade(
                trade_id=trade_id,
                close_date=day,
                close_price=px,
                pnl_pct=net,
                holding_days=held,
                close_reason=reason,
                gross_pnl_pct=gross,
                costs_pct=costs_pct,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("paper_twin_star close failed %s: %s", symbol, exc)
            continue
        if closed:
            summary["closed"] += 1
            reasons = summary["closeReasons"]
            reasons[reason] = int(reasons.get(reason) or 0) + 1
    return summary
