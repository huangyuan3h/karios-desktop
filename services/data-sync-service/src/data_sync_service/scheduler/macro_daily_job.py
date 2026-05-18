"""Full sync of macro/global daily series: run daily after US market close."""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.service.macro_daily import sync_macro_daily_full

logger = logging.getLogger(__name__)

JOB_ID = "macro_daily_full_sync"
CRON_EXPRESSION = "0 7 * * 2-6"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    result = sync_macro_daily_full()
    if result.get("ok"):
        if result.get("skipped"):
            logger.info("macro_daily_full_sync skipped: already synced today")
        else:
            logger.info("macro_daily_full_sync ok: updated=%s", result.get("updated", 0))
    else:
        logger.warning("macro_daily_full_sync failed: %s", result.get("error", "unknown"))