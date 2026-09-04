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

from data_sync_service.scheduler._job_guard import record_dict_result, run_guarded
from data_sync_service.service.hk_daily import sync_hk_daily_full

logger = logging.getLogger(__name__)

JOB_ID = "hk_daily_full_sync"
# Daily at 17:30 Asia/Shanghai (after HK market close at 16:00 HKT).
CRON_EXPRESSION = "30 17 * * *"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    result = run_guarded(JOB_ID, sync_hk_daily_full, log=logger)
    if result is None:
        return  # exception path already recorded + logged

    def _ok(r) -> None:
        if r.get("skipped"):
            logger.info("hk_daily_full_sync skipped: %s", r.get("message", ""))
        else:
            logger.info(
                "hk_daily_full_sync ok: updated=%s skipped=%s failed=%s",
                r.get("updated", 0),
                r.get("skipped_count", 0),
                r.get("failed_count", 0),
            )

    def _fail(r) -> None:
        logger.warning(
            "hk_daily_full_sync failed: %s last_ts_code=%s",
            r.get("error", "unknown"),
            r.get("last_ts_code"),
        )

    record_dict_result(JOB_ID, result, ok_log=_ok, fail_log=_fail)

