"""Sync ETF daily bars (fund_daily) after market close."""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger  # type: ignore[import-not-found]

from data_sync_service.service.etf_daily import sync_etf_daily_full

logger = logging.getLogger(__name__)

JOB_ID = "etf_daily_full_sync"
# Monthly on day 1 at 19:00 Asia/Shanghai (offset from HK daily at 18:30).
CRON_EXPRESSION = "0 19 1 * *"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    result = sync_etf_daily_full()
    if result.get("ok"):
        if result.get("skipped"):
            logger.info("etf_daily_full_sync skipped: already synced today")
        else:
            logger.info("etf_daily_full_sync ok: updated=%s", result.get("updated", 0))
    else:
        logger.warning("etf_daily_full_sync failed: %s", result.get("error", "unknown"))