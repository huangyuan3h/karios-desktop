"""Sync stock_basic from tushare every Friday 18:00 (Asia/Shanghai). Log on failure."""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.service.stock_basic import sync_stock_basic

logger = logging.getLogger(__name__)

JOB_ID = "stock_basic_sync"
# Friday 18:00 Asia/Shanghai
CRON_EXPRESSION = "0 18 * * 5"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    result = sync_stock_basic()
    if result.get("ok"):
        logger.info("stock_basic_sync ok: updated=%s", result.get("updated", 0))
    else:
        logger.warning("stock_basic_sync failed: %s", result.get("error", "unknown"))
