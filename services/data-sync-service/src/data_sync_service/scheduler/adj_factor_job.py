"""Fallback sync for adj_factor into daily table."""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.scheduler._job_guard import record_dict_result, run_guarded
from data_sync_service.service.adj_factor import sync_adj_factor_full

logger = logging.getLogger(__name__)

JOB_ID = "adj_factor_full_sync"
# Friday 18:30 Asia/Shanghai — moved off 17:00 where it collided with
# daily_sync (both hammered the shared tushare quota and both failed with
# "频率超限" every Friday since 2026-07-11). close_sync (17:10 daily) already
# refreshes adj_factor incrementally; this job is the historical backfill.
CRON_EXPRESSION = "30 18 * * 5"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    result = run_guarded(JOB_ID, sync_adj_factor_full, log=logger)
    if result is None:
        return  # exception path already recorded + logged

    def _ok(r) -> None:
        if r.get("skipped"):
            logger.info("adj_factor_full_sync skipped: already synced today")
        else:
            logger.info("adj_factor_full_sync ok: updated=%s", r.get("updated", 0))

    def _fail(r) -> None:
        logger.warning("adj_factor_full_sync failed: %s", r.get("error", "unknown"))

    record_dict_result(JOB_ID, result, ok_log=_ok, fail_log=_fail)

