"""Sync HK daily bars (hk_daily) after HK market close.

Runs every weekday after HK close (16:00 HKT). Each run performs an
**incremental** sync per ts_code (only rows newer than the last cached
trade_date), so the daily cost is small (~one day of bars per ticker).
If the run does not finish in one pass, the next day's cron resumes from
the last successful ts_code via the sync_job_record table.

Schedule: 17:30 Asia/Shanghai = ~1.5h after HK close to allow third-party
data sources to settle today's bars.
"""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger  # type: ignore[import-not-found]

from data_sync_service.service.hk_daily import sync_hk_daily_full

logger = logging.getLogger(__name__)

JOB_ID = "hk_daily_full_sync"
# Daily at 17:30 Asia/Shanghai (after HK market close at 16:00 HKT).
CRON_EXPRESSION = "30 17 * * *"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    result = sync_hk_daily_full()
    if result.get("ok"):
        if result.get("skipped"):
            logger.info("hk_daily_full_sync skipped: %s", result.get("message", ""))
        else:
            logger.info(
                "hk_daily_full_sync ok: updated=%s skipped=%s failed=%s",
                result.get("updated", 0),
                result.get("skipped_count", 0),
                result.get("failed_count", 0),
            )
    else:
        logger.warning(
            "hk_daily_full_sync failed: %s last_ts_code=%s",
            result.get("error", "unknown"),
            result.get("last_ts_code"),
        )

