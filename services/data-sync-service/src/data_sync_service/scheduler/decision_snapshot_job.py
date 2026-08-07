"""Decision archive snapshot job (18:00 Asia/Shanghai, weekdays, TIP-015 M3)."""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.db.sync_job_record import insert_record
from data_sync_service.service.decision import build_daily_snapshot

logger = logging.getLogger(__name__)

JOB_ID = "decision_snapshot"
CRON_EXPRESSION = "0 18 * * 1-5"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    try:
        rec = build_daily_snapshot()
        insert_record(JOB_ID, success=True)
        logger.info("decision snapshot: %s exchanges=%s", rec.get("snapshotDate"), rec.get("exchangeCount"))
    except Exception as exc:  # noqa: BLE001
        logger.exception("decision snapshot failed")
        insert_record(JOB_ID, success=False, error_message=str(exc))
