"""Decision outcome backfill job (weekdays after close, TIP-015 M3)."""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.db.sync_job_record import insert_record
from data_sync_service.service.decision import apply_daily_outcomes

logger = logging.getLogger(__name__)

JOB_ID = "decision_outcome"
CRON_EXPRESSION = "0 19 * * 1-5"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    try:
        result = apply_daily_outcomes(days=5)
        insert_record(JOB_ID, success=True)
        logger.info("decision outcome: updated=%s", result.get("updated"))
    except Exception as exc:  # noqa: BLE001
        logger.exception("decision outcome failed")
        insert_record(JOB_ID, success=False, error_message=str(exc))
