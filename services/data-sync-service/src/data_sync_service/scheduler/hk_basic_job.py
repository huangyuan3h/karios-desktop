"""Sync hk_basic from tushare monthly (Asia/Shanghai). Log on failure."""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger  # type: ignore[import-not-found]

from data_sync_service.scheduler._job_guard import record_dict_result, run_guarded
from data_sync_service.service.hk_basic import sync_hk_basic

logger = logging.getLogger(__name__)

JOB_ID = "hk_basic_sync"
# Monthly on day 1 at 03:30 Asia/Shanghai.
CRON_EXPRESSION = "30 3 1 * *"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    result = run_guarded(JOB_ID, sync_hk_basic, log=logger)
    if result is None:
        return  # exception path already recorded + logged

    def _ok(r) -> None:
        if r.get("skipped"):
            logger.info("hk_basic_sync skipped: already synced this month")
        else:
            logger.info("hk_basic_sync ok: updated=%s", r.get("updated", 0))

    def _fail(r) -> None:
        logger.warning("hk_basic_sync failed: %s", r.get("error", "unknown"))

    record_dict_result(JOB_ID, result, ok_log=_ok, fail_log=_fail)

