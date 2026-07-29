"""Sync HK stock industry labels (Xueqiu mbu) daily at 02:00 Asia/Shanghai."""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger  # type: ignore[import-not-found]

from data_sync_service.service.hk_industry import get_hk_industry_status, sync_hk_industry

logger = logging.getLogger(__name__)

JOB_ID = "hk_industry_sync"
# Daily at 02:00 Asia/Shanghai — before HK market open (09:30 HKT) so watchlist is ready.
CRON_EXPRESSION = "0 2 * * *"
TIMEZONE = "Asia/Shanghai"
# Default batch size — keep small to avoid Xueqiu rate limits on a daily cron.
BATCH_LIMIT = 200


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    """Fill missing HK industry labels (up to BATCH_LIMIT per day)."""
    status = get_hk_industry_status()
    missing = int(status.get("missingHk", 0) or 0)
    if missing <= 0:
        logger.info("hk_industry_sync skipped: all HK codes mapped")
        return
    result = sync_hk_industry(limit=min(BATCH_LIMIT, missing))
    if result.get("ok"):
        if result.get("skipped"):
            logger.info("hk_industry_sync skipped: %s", result.get("message", "no HK codes to update"))
        else:
            logger.info(
                "hk_industry_sync ok: requested=%s resolved=%s updated=%s",
                result.get("requested", 0),
                result.get("resolved", 0),
                result.get("updated", 0),
            )
    else:
        logger.warning("hk_industry_sync failed: %s", result.get("error", "unknown"))