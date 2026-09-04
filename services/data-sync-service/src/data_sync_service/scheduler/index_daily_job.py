"""Full sync of index daily bars: run daily after A-share close."""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.scheduler._job_guard import record_dict_result, run_guarded
from data_sync_service.service.index_daily import sync_index_daily_full

logger = logging.getLogger(__name__)

JOB_ID = "index_daily_full_sync"
CRON_EXPRESSION = "30 16 * * 1-5"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    result = run_guarded(JOB_ID, sync_index_daily_full, log=logger)
    if result is None:
        return  # exception path already recorded + logged

    def _ok(r) -> None:
        if r.get("skipped"):
            logger.info("index_daily_full_sync skipped: already synced today")
        else:
            logger.info("index_daily_full_sync ok: updated=%s", r.get("updated", 0))

    def _fail(r) -> None:
        logger.warning("index_daily_full_sync failed: %s", r.get("error", "unknown"))

    record_dict_result(JOB_ID, result, ok_log=_ok, fail_log=_fail)