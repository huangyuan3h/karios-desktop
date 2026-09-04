"""Sync stock_basic from tushare every Friday 18:00 (Asia/Shanghai). Log on failure."""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.scheduler._job_guard import record_dict_result, run_guarded
from data_sync_service.service.stock_basic import sync_stock_basic

logger = logging.getLogger(__name__)

JOB_ID = "stock_basic_sync"
# Friday 18:00 Asia/Shanghai
CRON_EXPRESSION = "0 18 * * 5"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    result = run_guarded(JOB_ID, sync_stock_basic, log=logger)
    if result is None:
        return  # exception path already recorded + logged

    def _ok(r) -> None:
        logger.info("stock_basic_sync ok: updated=%s", r.get("updated", 0))

    def _fail(r) -> None:
        logger.warning("stock_basic_sync failed: %s", r.get("error", "unknown"))

    record_dict_result(JOB_ID, result, ok_log=_ok, fail_log=_fail)
