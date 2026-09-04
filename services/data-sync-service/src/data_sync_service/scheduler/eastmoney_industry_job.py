"""Incremental East Money industry prewarm: weekdays after market close."""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.scheduler._job_guard import record_dict_result, run_guarded
from data_sync_service.service.eastmoney_industry import sync_eastmoney_industry_incremental

logger = logging.getLogger(__name__)

JOB_ID = "eastmoney_industry_sync"
# Weekdays 18:00 Asia/Shanghai (after close_sync 17:10)
CRON_EXPRESSION = "0 18 * * 1-5"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    result = run_guarded(
        JOB_ID,
        lambda: sync_eastmoney_industry_incremental(mode="missing", batch_size=1000, max_batches=1),
        log=logger,
    )
    if result is None:
        return  # exception path already recorded + logged

    def _ok(r) -> None:
        if r.get("skipped"):
            logger.info(
                "eastmoney_industry_sync skipped: %s (coverage=%s%%)",
                r.get("message", ""),
                r.get("coveragePct", 0),
            )
        else:
            logger.info(
                "eastmoney_industry_sync ok: requested=%s resolved=%s updated=%s coverage=%s%%",
                r.get("requested", 0),
                r.get("resolved", 0),
                r.get("updated", 0),
                r.get("coveragePct", 0),
            )

    def _fail(r) -> None:
        logger.warning(
            "eastmoney_industry_sync failed: %s",
            r.get("error", "unknown"),
        )

    record_dict_result(JOB_ID, result, ok_log=_ok, fail_log=_fail)
