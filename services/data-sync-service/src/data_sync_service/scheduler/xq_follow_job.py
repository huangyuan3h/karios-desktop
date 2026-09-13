"""Daily Snowball (雪球) follow-count snapshot on weekdays at 15:40 Asia/Shanghai.

Runs just after the A-share close so the cumulative follow cross-section is
captured once per trading day. There is no history API — the panel only grows
forward, so missing a day is an irrecoverable gap (P0-13 attention line).
"""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.scheduler._job_guard import record_dict_result, run_guarded
from data_sync_service.service.xq_follow import sync_xq_follow_snapshot

logger = logging.getLogger(__name__)

JOB_ID = "xq_follow_snapshot"
# Weekdays 15:40 Asia/Shanghai (after the 15:00 close).
CRON_EXPRESSION = "40 15 * * 1-5"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    result = run_guarded(JOB_ID, sync_xq_follow_snapshot, log=logger)
    if result is None:
        return  # exception path already recorded + logged

    def _ok(r) -> None:
        logger.info("xq_follow_snapshot ok: updated=%s trade_date=%s",
                    r.get("updated", 0), r.get("trade_date", ""))

    def _fail(r) -> None:
        logger.warning("xq_follow_snapshot failed: %s", r.get("error", "unknown"))

    record_dict_result(JOB_ID, result, ok_log=_ok, fail_log=_fail)
