"""Sync HK daily bars (hk_daily) after market close."""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger  # type: ignore[import-not-found]

from data_sync_service.service.hk_daily import sync_hk_daily_full

logger = logging.getLogger(__name__)

JOB_ID = "hk_daily_full_sync"
# Monthly on day 1 at 18:30 Asia/Shanghai.
CRON_EXPRESSION = "30 18 1 * *"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    result = sync_hk_daily_full()
    if result.get("ok"):
        if result.get("skipped"):
            logger.info("hk_daily_full_sync skipped: already synced today")
        else:
            logger.info("hk_daily_full_sync ok: updated=%s", result.get("updated", 0))
    else:
        logger.warning("hk_daily_full_sync failed: %s", result.get("error", "unknown"))

