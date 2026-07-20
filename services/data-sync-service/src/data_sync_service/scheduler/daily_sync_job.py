"""Full sync of daily bars: run daily; on failure log only."""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.service.close_sync import sync_close

logger = logging.getLogger(__name__)

JOB_ID = "daily_full_sync"
# Redirected to close_sync (legacy per-stock daily_full deprecated).
CRON_EXPRESSION = "0 17 * * 5"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    result = sync_close(exchange="SSE", force=False)
    if isinstance(result, dict) and result.get("ok"):
        if result.get("skipped"):
            logger.info("daily_full_sync (close_sync) skipped: already synced today")
        else:
            logger.info("daily_full_sync (close_sync) ok: %s", result)
    elif isinstance(result, dict):
        logger.warning("daily_full_sync (close_sync) failed: %s", result.get("error", "unknown"))
    else:
        logger.info("daily_full_sync (close_sync) completed")
