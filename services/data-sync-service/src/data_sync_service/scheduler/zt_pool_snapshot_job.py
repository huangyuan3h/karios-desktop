"""Daily East Money limit-up pool snapshot on weekdays at 17:45 Asia/Shanghai.

The pool API keeps only ~2 weeks, so missing a day is an irrecoverable gap; this
grows the 封板资金/首封时间/炸板次数/连板数 panel forward (P0-13 B18 follow-up).
"""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.scheduler._job_guard import record_dict_result, run_guarded
from data_sync_service.service.zt_pool_snapshot import snapshot_recent

logger = logging.getLogger(__name__)

JOB_ID = "zt_pool_snapshot"
# Weekdays 17:45 Asia/Shanghai (after cn_industry_post_close at 17:35).
CRON_EXPRESSION = "45 17 * * 1-5"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    result = run_guarded(JOB_ID, snapshot_recent, log=logger)
    if result is None:
        return  # exception path already recorded + logged

    def _ok(r) -> None:
        logger.info(
            "zt_pool_snapshot ok: saved=%s skipped=%s failed=%s",
            r.get("saved", []),
            r.get("skipped", []),
            r.get("failed", []),
        )

    def _fail(r) -> None:
        logger.warning("zt_pool_snapshot failed: %s", r.get("error", "unknown"))

    record_dict_result(JOB_ID, result, ok_log=_ok, fail_log=_fail)
