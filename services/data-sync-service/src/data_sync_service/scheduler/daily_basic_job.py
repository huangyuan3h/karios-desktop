"""Incremental sync of stock_dailybasic (total_mv / circ_mv / turnover_rate).

Weekdays 17:20 Asia/Shanghai — right after index_basic_sync (17:15). The
Twin-Star (双子星) satellite reads stock_dailybasic.total_mv every day to pick
S-gap low-volatility candidates; the table was orphaned (last write 2026-08-07)
before this job existed. Idempotent per (ts_code, trade_date).
"""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.db.stock_dailybasic import sync_daily_basic_gap
from data_sync_service.scheduler._job_guard import record_dict_result, run_guarded

logger = logging.getLogger(__name__)

JOB_ID = "stock_daily_basic_sync"
# Weekdays 17:20 Asia/Shanghai (after index_basic_sync 17:15).
CRON_EXPRESSION = "20 17 * * 1-5"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    result = run_guarded(JOB_ID, sync_daily_basic_gap, log=logger)
    if result is None:
        return  # exception path already recorded + logged

    def _ok(r) -> None:
        if r.get("skipped"):
            logger.info("stock_daily_basic_sync skipped: %s", r.get("message", ""))
        else:
            logger.info(
                "stock_daily_basic_sync ok: updated=%s days=%s",
                r.get("updated", 0),
                r.get("days", 0),
            )

    def _fail(r) -> None:
        logger.warning("stock_daily_basic_sync failed: %s", r.get("error", "unknown"))

    record_dict_result(JOB_ID, result, ok_log=_ok, fail_log=_fail)