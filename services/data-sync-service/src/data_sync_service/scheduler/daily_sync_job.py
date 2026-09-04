"""Full sync of daily bars: run daily; on failure log only."""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.scheduler._job_guard import (
    record_dict_result,
    record_failure,
)
from data_sync_service.service.close_sync import sync_close

logger = logging.getLogger(__name__)

JOB_ID = "daily_full_sync"
# Redirected to close_sync (legacy per-stock daily_full deprecated).
CRON_EXPRESSION = "0 17 * * 5"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    # NOTE: sync_close may legitimately return None (legacy Friday path), and
    # run_guarded uses None for the exception path — so this job maps
    # explicitly instead of run_guarded.
    try:
        result = sync_close(exchange="SSE", force=False)
    except Exception as exc:  # noqa: BLE001
        record_failure(JOB_ID, exc)
        logger.exception("daily_full_sync (close_sync) failed: %s", exc)
        return

    def _ok(r) -> None:
        if isinstance(r, dict) and r.get("skipped"):
            logger.info("daily_full_sync (close_sync) skipped: already synced today")
        elif isinstance(r, dict):
            logger.info("daily_full_sync (close_sync) ok: %s", r)
        else:
            logger.info("daily_full_sync (close_sync) completed")

    def _fail(r) -> None:
        logger.warning("daily_full_sync (close_sync) failed: %s", r.get("error", "unknown"))

    record_dict_result(JOB_ID, result, ok_log=_ok, fail_log=_fail)
