"""Fallback sync for adj_factor into daily table."""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.service.adj_factor import sync_adj_factor_full

logger = logging.getLogger(__name__)

JOB_ID = "adj_factor_full_sync"
# Every Friday 17:00 Asia/Shanghai (fallback; other strategy may be used normally)
CRON_EXPRESSION = "0 17 * * 5"
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

