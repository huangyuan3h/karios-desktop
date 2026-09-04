"""Daily morphology factor scan (strong_scoop_exhaustion): weekdays post-close.

Scans the latest trading day and persists signals into factor_signals
(direction-only layer; never touches S-3). Previously only triggered via
POST /factors/sync, so the table held a single backfill day (2026-09-04 audit).
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.scheduler._job_guard import record_dict_result, run_guarded
from data_sync_service.service.factor_signals_service import scan_strong_scoop_exhaustion

logger = logging.getLogger(__name__)

JOB_ID = "factor_signals_sync"
# Weekdays 18:30 Asia/Shanghai (after close_sync 17:10; needs today's daily bars).
CRON_EXPRESSION = "30 18 * * 1-5"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def _latest_open_date() -> str:
    from data_sync_service.db.trade_calendar import get_open_dates

    today = date.today()
    opens = get_open_dates("SSE", today - timedelta(days=7), today)
    if opens:
        return opens[-1].isoformat()
    return today.isoformat()


def _scan() -> dict:
    target = _latest_open_date()
    n = scan_strong_scoop_exhaustion(target)
    return {"ok": True, "trade_date": target, "signals": n}


def run() -> None:
    result = run_guarded(JOB_ID, _scan, log=logger)
    if result is None:
        return  # exception path already recorded + logged

    def _ok(r) -> None:
        logger.info(
            "factor_signals_sync ok: trade_date=%s signals=%s",
            r.get("trade_date"),
            r.get("signals", 0),
        )

    def _fail(r) -> None:
        logger.warning(
            "factor_signals_sync failed: %s",
            r.get("error", "unknown"),
        )

    record_dict_result(JOB_ID, result, ok_log=_ok, fail_log=_fail)
