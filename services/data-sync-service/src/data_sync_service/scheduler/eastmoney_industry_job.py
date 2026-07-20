"""Incremental East Money industry prewarm: weekdays after market close."""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.service.eastmoney_industry import sync_eastmoney_industry_incremental

logger = logging.getLogger(__name__)

JOB_ID = "eastmoney_industry_sync"
# Weekdays 18:00 Asia/Shanghai (after close_sync 17:10)
CRON_EXPRESSION = "0 18 * * 1-5"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    result = sync_eastmoney_industry_incremental(mode="missing", batch_size=1000, max_batches=1)
    if result.get("ok"):
        if result.get("skipped"):
            logger.info(
                "eastmoney_industry_sync skipped: %s (coverage=%s%%)",
                result.get("message", ""),
                result.get("coveragePct", 0),
            )
        else:
            logger.info(
                "eastmoney_industry_sync ok: requested=%s resolved=%s updated=%s coverage=%s%%",
                result.get("requested", 0),
                result.get("resolved", 0),
                result.get("updated", 0),
                result.get("coveragePct", 0),
            )
    else:
        logger.warning(
            "eastmoney_industry_sync failed: %s",
            result.get("error", "unknown"),
        )
