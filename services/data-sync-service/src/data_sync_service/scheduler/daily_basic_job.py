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

logger = logging.getLogger(__name__)

JOB_ID = "stock_daily_basic_sync"
# Weekdays 17:20 Asia/Shanghai (after index_basic_sync 17:15).
CRON_EXPRESSION = "20 17 * * 1-5"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    result = sync_daily_basic_gap()
    if result.get("ok"):
        if result.get("skipped"):
            logger.info("stock_daily_basic_sync skipped: %s", result.get("message", ""))
        else:
            logger.info(
                "stock_daily_basic_sync ok: updated=%s days=%s",
                result.get("updated", 0),
                result.get("days", 0),
            )
    else:
        logger.warning("stock_daily_basic_sync failed: %s", result.get("error", "unknown"))