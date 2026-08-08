"""Fallback sync for adj_factor into daily table."""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.service.adj_factor import sync_adj_factor_full

logger = logging.getLogger(__name__)

JOB_ID = "adj_factor_full_sync"
# Friday 18:30 Asia/Shanghai — moved off 17:00 where it collided with
# daily_sync (both hammered the shared tushare quota and both failed with
# "频率超限" every Friday since 2026-07-11). close_sync (17:10 daily) already
# refreshes adj_factor incrementally; this job is the historical backfill.
CRON_EXPRESSION = "30 18 * * 5"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    result = sync_adj_factor_full()
    if result.get("ok"):
        if result.get("skipped"):
            logger.info("adj_factor_full_sync skipped: already synced today")
        else:
            logger.info("adj_factor_full_sync ok: updated=%s", result.get("updated", 0))
    else:
        logger.warning("adj_factor_full_sync failed: %s", result.get("error", "unknown"))

