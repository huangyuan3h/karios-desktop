"""TradingView screener capture at the AM/PM snapshot points.

Enqueues all enabled screeners into the tv_capture_jobs queue; the in-process
tv_capture_worker picks them up. We register two cron entries (09:30 AM and
15:30 PM) sharing this same handler — matching docs/modules/screener.md
"系统每天保存两个时段的快照：AM / PM" intent that was previously on-demand only.
"""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.db.sync_job_record import insert_record
from data_sync_service.service.dashboard import _sync_screeners_step

logger = logging.getLogger(__name__)

JOB_ID_AM = "tv_screener_capture_am"
JOB_ID_PM = "tv_screener_capture_pm"
TIMEZONE = "Asia/Shanghai"


def build_am_trigger() -> CronTrigger:
    # Weekdays 09:30 Asia/Shanghai (A-share morning open).
    return CronTrigger.from_crontab("30 9 * * 1-5", timezone=TIMEZONE)


def build_pm_trigger() -> CronTrigger:
    # Weekdays 15:30 Asia/Shanghai (right after A-share close).
    return CronTrigger.from_crontab("30 15 * * 1-5", timezone=TIMEZONE)


def run() -> None:
    try:
        result = _sync_screeners_step(screeners_enabled=True)
        if result.get("skipped"):
            insert_record(JOB_ID_AM, success=True, error_message="no enabled screeners")
            logger.info("tv_screener_capture skipped: %s", result.get("message", ""))
            return
        failed = int(result.get("failed") or 0)
        missing = int(result.get("missing") or 0)
        ok = failed == 0 and missing == 0
        msg = f"enabled={result.get('enabled')} failed={failed} missing={missing}"
        insert_record(JOB_ID_AM, success=ok, error_message=None if ok else msg)
        if ok:
            logger.info("tv_screener_capture ok: %s", msg)
        else:
            logger.warning("tv_screener_capture failed: %s", msg)
    except Exception as e:
        insert_record(JOB_ID_AM, success=False, error_message=str(e))
        logger.warning("tv_screener_capture crashed: %s", e)