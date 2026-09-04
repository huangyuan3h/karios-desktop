"""clip4 satellite paper book (OPT-131) — independent of S-3 paper.

Occupancy for live Watchlist is the broker book. This module records what the
habit recipe does: at most 4 slots × 12.5% NAV, body=3 with C1 entry filter
(14:30/open-1 > 3% skip, strict no refill) and day-3 14:30 exit.
No protect stop, pyramid, trailing, or 60-day hold.

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
)
from data_sync_service.service.paper_cost_model import round_trip_cost_pct
from data_sync_service.service.state_bucket_track import BODY, MAX_POS, POSITION_PCT
from data_sync_service.service.trade_calendar_utils import (
    is_cn_trading_day,
    shanghai_today_iso,
)
from data_sync_service.service.twin_star_daily import (
    HABIT_C1_PCT,
    HABIT_EXIT_HHMM,
    SAT_SLOT_NAV_PCT,
    build_twin_star_daily_action,
    cn_symbol_from_ts,
    count_sessions_inclusive,
)

logger = logging.getLogger(__name__)

SLEEVE_PCT = POSITION_PCT * 0.5  # 0.125 of NAV when sat sleeve is 50%
HABIT_RECIPE = "clip4_habit_c1_x1430"


def _ensure_5min_today(ts_codes: list[str], day: str) -> None:
    """Best-effort import of today's last-hour 5min bars before paper pricing.

    The 18:40 bar_5min job runs after the 17:43 paper job, so without this
    the 14:30 print lookup always misses and silently falls back to the
    daily close. Failures only warn; callers fall back and record the source.
    """
    codes = [c for c in dict.fromkeys(ts_codes) if c]
    if not codes:
        return
    try:
        from data_sync_service.service.bar_5min import SOURCE_BAOSTOCK, backfill_symbols

        res = backfill_symbols(
            ts_codes=codes, start_date=day, end_date=day,
            source=SOURCE_BAOSTOCK, skip_covered=True,
        )
        logger.info(
            "paper_twin_star 5min ensure %s: ok=%s stored=%s failed=%s skipped=%s",
            day, res.get("ok"), res.get("stored"), res.get("failed"), res.get("skipped"),
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("paper_twin_star 5min ensure failed %s: %s", day, exc)


def _fetch_1430_px_map(ts_codes: list[str], day: str) -> tuple[dict[str, float], dict[str, str]]:
    """14:30 prints from bar_5min. Returns (px_by_ts, source_by_ts)."""
    px: dict[str, float] = {}
    src: dict[str, str] = {}
    if ts_codes:
        try:
            import psycopg

            from data_sync_service.config import get_settings

            conn = psycopg.connect(get_settings().database_url)
            try:
                cur = conn.cursor()
                cur.execute(
                    "SELECT ts_code, close FROM bar_5min "
                    "WHERE trade_time = '1430' AND trade_date = %s "
                    "AND ts_code = ANY(%s) AND close IS NOT NULL AND close > 0",
                    (day, ts_codes),
                )
                for ts, c in cur.fetchall():
                    px[str(ts)] = float(c)
                    src[str(ts)] = "bar_5min_1430"
            finally:
                conn.close()
        except Exception as exc:  # noqa: BLE001
            logger.warning("paper_twin_star 1430 exit fetch failed %s: %s", day, exc)
    return px, src


def _open_twin_star() -> list[dict[str, Any]]:
    rows = list_paper_trades(status="open", market="CN", limit=50)
    return [r for r in rows if str(r.get("source") or "") == SOURCE_TWIN_STAR]


def _held_days(entry_date: str | None, as_of: str) -> int:
    if not entry_date:
        return 0
    return count_sessions_inclusive(str(entry_date), as_of)


def run_intake_twin_star(*, trade_date: str | None = None) -> dict[str, Any]:
    """Insert habit sat candidates as paper BUYs. Cap 4. No refill/pyramid.

    Candidates arrive C1-filtered from the intraday action (14:30/open-1 > 3%
    skipped, strict). Entry px is the 14:30 print carried in cand.close.
    """
    from datetime import date as _date

    day = trade_date or shanghai_today_iso()
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
        session_flag = is_cn_trading_day(_date.fromisoformat(day))
    except Exception:  # noqa: BLE001
        session_flag = None
    if session_flag is False:
        _skip("non_session")
        return summary

    try:
        action = build_twin_star_daily_action()
    except Exception as exc:  # noqa: BLE001
        summary["error"] = f"twin_star action failed: {exc}"
        logger.warning("paper_twin_star intake action failed: %s", exc)
        return summary

    sat = action.get("sat") or {}
    if sat.get("snapshotMissing") or sat.get("snapshotStale"):
        # Today's tape never arrived: the cached action falls back to the T-1
        # signal list, which must not be bought at 14:30 prices. Skip loudly.
        _skip("snapshot_bad")
        summary["snapshotReason"] = sat.get("snapshotReason")
        logger.warning(
            "paper_twin_star intake skipped: snapshot bad (%s)",
            sat.get("snapshotReason"),
        )
        return summary
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
    _ensure_5min_today(ts_codes, day)
    px_1430, _ = _fetch_1430_px_map(ts_codes, day)
    closes: dict[str, float] = {}
    close_src: dict[str, str] = {}
    try:
        bars = fetch_last_ohlcv_batch(ts_codes, days=5)
        for ts, rows in (bars or {}).items():
            if not rows:
                continue
            close = rows[-1][4] if len(rows[-1]) >= 5 else None
            if close is not None:
                closes[str(ts)] = float(close)
                close_src[str(ts)] = "daily_close"
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
        # Habit entry print priority: real 14:30 bar > snapshot 14:30
        # estimate > daily close. Source is recorded for the C4对照.
        px = closes.get(ts)
        px_src = close_src.get(ts, "daily_close")
        if cand.get("close") is not None:
            try:
                px = float(cand["close"])
                px_src = "snapshot_close_1430est"
            except (TypeError, ValueError):
                pass
        if ts in px_1430:
            px = px_1430[ts]
            px_src = "bar_5min_1430"
        if px is None or px <= 0:
            _skip("no_price")
            continue
        src_counts = summary.setdefault("entryPxSrc", {})
        src_counts[px_src] = int(src_counts.get(px_src) or 0) + 1
        try:
            row = insert_paper_trade(
                symbol=symbol,
                entry_date=day,
                side="BUY",
                entry_price=px,
                why_at_entry="twin_star S-gap habit C1 14:30",
                sleeve_pct=SLEEVE_PCT,
                source=SOURCE_TWIN_STAR,
                market="CN",
                signal_snapshot={
                    "recipe": HABIT_RECIPE,
                    "body": BODY,
                    "slotNavPct": SAT_SLOT_NAV_PCT,
                    "amp": cand.get("amp"),
                    "gapPct": cand.get("gapPct"),
                    "runUpPct": cand.get("runUpPct"),
                    "openPx": cand.get("openPx"),
                    "c1Pct": HABIT_C1_PCT,
                    "exitHhmm": HABIT_EXIT_HHMM,
                    "entryPx": px,
                    "entryPxSrc": px_src,
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
    """Close habit paper at body=3 day-3 14:30 print. No protect stop."""
    day = today_iso_s or shanghai_today_iso()
    summary: dict[str, Any] = {
        "today": day,
        "scanned": 0,
        "closed": 0,
        "closeReasons": {},
        "exitHhmm": HABIT_EXIT_HHMM,
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
    _ensure_5min_today(ts_codes, day)
    closes: dict[str, float] = {}
    try:
        bars = fetch_last_ohlcv_batch(ts_codes, days=8)
        for ts, rows in (bars or {}).items():
            if rows:
                closes[str(ts)] = float(rows[-1][4])
    except Exception as exc:  # noqa: BLE001
        logger.warning("paper_twin_star update fetch failed: %s", exc)
    # Habit exit print first: bar_5min 14:30 on the exit day.
    exit_1430, _exit_src = _fetch_1430_px_map(ts_codes, day)
    for ts, px1430 in exit_1430.items():
        closes[ts] = px1430

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
        exit_src = "bar_5min_1430" if (ts or "") in exit_1430 else "daily_close"
        src_counts = summary.setdefault("exitPxSrc", {})
        src_counts[exit_src] = int(src_counts.get(exit_src) or 0) + 1
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
                signal_snapshot_extra={"exitPx": px, "exitPxSrc": exit_src},
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("paper_twin_star close failed %s: %s", symbol, exc)
            continue
        if closed:
            summary["closed"] += 1
            reasons = summary["closeReasons"]
            reasons[reason] = int(reasons.get(reason) or 0) + 1
    return summary
