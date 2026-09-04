"""Full sync of index_dailybasic (market breadth) on weekdays at 17:15 Asia/Shanghai.

Runs independently of the close_sync chain so that macro_snapshot.market_breadth
(turnover_rate / float_mv from index_dailybasic) is warm before the dashboard
17:35 reads without relying on the user clicking "Sync all".
"""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.scheduler._job_guard import record_dict_result, run_guarded
from data_sync_service.service.index_basic import sync_index_basic_full

logger = logging.getLogger(__name__)

JOB_ID = "index_basic_sync"
# Weekdays 17:15 Asia/Shanghai (after close_sync at 17:10, before 17:35 post-close chain).
CRON_EXPRESSION = "15 17 * * 1-5"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    result = run_guarded(JOB_ID, sync_index_basic_full, log=logger)
    if result is None:
        return  # exception path already recorded + logged

    def _ok(r) -> None:
        if r.get("skipped"):
            logger.info("index_basic_sync skipped: %s", r.get("message", ""))
        else:
            logger.info(
                "index_basic_sync ok: updated=%s",
                r.get("updated", 0),
            )

    def _fail(r) -> None:
        logger.warning("index_basic_sync failed: %s", r.get("error", "unknown"))

    record_dict_result(JOB_ID, result, ok_log=_ok, fail_log=_fail)