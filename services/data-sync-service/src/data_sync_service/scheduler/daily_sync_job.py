"""Full sync of daily bars: run daily; on failure log only."""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.service.daily import sync_daily_full

logger = logging.getLogger(__name__)

JOB_ID = "daily_full_sync"
# Every Friday 17:00 Asia/Shanghai (fallback; other sync strategy will be used normally)
CRON_EXPRESSION = "0 17 * * 5"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    result = sync_daily_full()
    if result.get("ok"):
        if result.get("skipped"):
            logger.info("daily_full_sync skipped: already synced today")
        else:
            logger.info("daily_full_sync ok: updated=%s", result.get("updated", 0))
    else:
        logger.warning("daily_full_sync failed: %s", result.get("error", "unknown"))
