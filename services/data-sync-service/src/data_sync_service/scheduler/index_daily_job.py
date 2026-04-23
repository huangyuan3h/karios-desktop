"""Full sync of index daily bars: run daily after A-share close."""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.service.index_daily import sync_index_daily_full

logger = logging.getLogger(__name__)

JOB_ID = "index_daily_full_sync"
CRON_EXPRESSION = "30 16 * * 1-5"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    result = sync_index_daily_full()
    if result.get("ok"):
        if result.get("skipped"):
            logger.info("index_daily_full_sync skipped: already synced today")
        else:
            logger.info("index_daily_full_sync ok: updated=%s", result.get("updated", 0))
    else:
        logger.warning("index_daily_full_sync failed: %s", result.get("error", "unknown"))