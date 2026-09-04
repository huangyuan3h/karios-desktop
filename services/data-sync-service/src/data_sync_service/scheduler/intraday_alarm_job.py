"""Intraday drawdown alarm (E3 · todo §14 #3 · P1 · 2026-08-12).

Hourly during CN trading hours (10:00-14:00 Asia/Shanghai weekdays) —
user's decision: hourly is enough because broker conditional orders already
guard the extreme intraday case; this alarm is a backstop reminder.

Pulls open paper trades, merges realtime quotes, and emits an
`intraday_drawdown` webhook event when the latest price is <= -8% from
entry (once per symbol per day via dedupe_key).
"""

from __future__ import annotations

import logging
from datetime import date

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.db import paper_trading
from data_sync_service.db.webhook import emit_event
from data_sync_service.scheduler._job_guard import (
    record_failure,
    record_success,
    run_guarded,
)
from data_sync_service.service.paper_trading import _resolve_ts_code
from data_sync_service.service.realtime_quote import fetch_realtime_quotes

logger = logging.getLogger(__name__)

JOB_ID = "intraday_alarm"
CRON_EXPRESSION = "0 10,11,12,13,14 * * 1-5"
TIMEZONE = "Asia/Shanghai"
DRAWDOWN_THRESHOLD_PCT = -8.0


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def check_intraday_drawdowns() -> dict:
    """Scan open paper trades against realtime quotes; emit alarms."""
    trades = paper_trading.get_open_paper_trades()
    ts_map: dict[str, str] = {}
    for t in trades:
        resolved = _resolve_ts_code(str(t.get("symbol") or ""))
        if resolved:
            ts_map[resolved[1]] = str(t.get("symbol") or "")
    if not ts_map:
        return {"ok": True, "checked": 0, "alarms": 0, "skipped": 0}

    quotes = fetch_realtime_quotes(list(ts_map))
    if not quotes.get("ok"):
        return {"ok": False, "error": quotes.get("error", "quote fetch failed")}

    alarms = 0
    skipped = 0
    for q in quotes.get("items", []):
        symbol = ts_map.get(str(q.get("ts_code") or ""))
        if not symbol:
            continue
        try:
            price = float(q.get("price") or 0)
        except (TypeError, ValueError):
            skipped += 1
            continue
        trade = next((t for t in trades if str(t.get("symbol")) == symbol), None)
        if trade is None:
            continue
        entry = float(trade.get("entry_price") or 0)
        if entry <= 0 or price <= 0:
            skipped += 1
            continue
        drawdown_pct = (price - entry) / entry * 100.0
        if drawdown_pct <= DRAWDOWN_THRESHOLD_PCT:
            emit_event(
                "intraday_drawdown",
                {
                    "symbol": symbol,
                    "ts_code": q.get("ts_code"),
                    "entry_price": entry,
                    "price": price,
                    "drawdown_pct": round(drawdown_pct, 2),
                    "threshold_pct": DRAWDOWN_THRESHOLD_PCT,
                },
                dedupe_key=f"intraday_drawdown:{symbol}:{date.today().isoformat()}",
            )
            alarms += 1
    return {"ok": True, "checked": len(trades), "alarms": alarms, "skipped": skipped}


def run() -> None:
    result = run_guarded(JOB_ID, check_intraday_drawdowns, log=logger)
    if result is None:
        return  # exception path already recorded + logged
    if result.get("alarms"):
        logger.info("intraday alarm: %s", result)
    if result.get("ok", True):
        record_success(
            JOB_ID,
            last_ts_code=f"checked={result.get('checked', 0)} alarms={result.get('alarms', 0)}",
        )
    else:
        logger.warning("intraday alarm quotes failed: %s", result.get("error", "unknown"))
        record_failure(JOB_ID, result.get("error", "unknown"))
