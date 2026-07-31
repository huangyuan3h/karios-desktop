"""Sync HK stock industry labels (East Money primary, Xueqiu fallback) daily at 02:00 Asia/Shanghai."""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger  # type: ignore[import-not-found]

from data_sync_service.service.hk_industry import get_hk_industry_status, sync_hk_industry

logger = logging.getLogger(__name__)

JOB_ID = "hk_industry_sync"
# Daily at 02:00 Asia/Shanghai — before HK market open (09:30 HKT) so watchlist is ready.
CRON_EXPRESSION = "0 2 * * *"
TIMEZONE = "Asia/Shanghai"
# East Money page-size is 500; the full HK universe fits in 14 pages.
# Use a generous cap so a single daily run also covers warrants / prefs that
# the cache may not have seen yet.
BATCH_LIMIT = 5000


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    """Refill / refresh HK industry labels (EM primary, Xueqiu fallback)."""
    status = get_hk_industry_status()
    missing = int(status.get("missingHk", 0) or 0)
    mapped = int(status.get("mappedHk", 0) or 0)
    total = int(status.get("totalHk", 0) or 0)
    # Always run — EM labels may have been updated upstream.
    result = sync_hk_industry(limit=min(BATCH_LIMIT, max(missing, total)))
    if result.get("ok"):
        if result.get("skipped"):
            logger.info("hk_industry_sync skipped: %s", result.get("message", "no HK codes to update"))
        else:
            logger.info(
                "hk_industry_sync ok: resolved=%s updated=%s emResolved=%s xueqiuResolved=%s pages=%s",
                result.get("resolved", 0),
                result.get("updated", 0),
                result.get("emResolved", 0),
                result.get("xueqiuResolved", 0),
                result.get("emPages", 0),
            )
    else:
        logger.warning("hk_industry_sync failed: %s", result.get("error", "unknown"))