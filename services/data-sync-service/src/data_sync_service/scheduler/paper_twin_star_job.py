"""clip4 satellite paper book cron.

Weekdays 17:43 Asia/Shanghai — after paper_s3_intake (17:42), before
paper_trading_update (17:45). S-3 update skips source=twin_star rows.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.db.sync_job_record import insert_record
from data_sync_service.service.paper_twin_star import run_intake_twin_star, run_update_twin_star

logger = logging.getLogger(__name__)

JOB_ID = "paper_twin_star"
CRON_EXPRESSION = "43 17 * * 1-5"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def _today_iso_utc() -> str:
    return datetime.now(tz=UTC).date().isoformat()


def run() -> None:
    today = _today_iso_utc()
    try:
        intake = run_intake_twin_star(trade_date=today)
        update = run_update_twin_star(today_iso_s=today)
    except Exception as exc:  # noqa: BLE001
        insert_record(JOB_ID, success=False, error_message=str(exc))
        logger.warning("paper_twin_star failed: %s", exc)
        return
    err = intake.get("error") or update.get("error")
    if err:
        insert_record(JOB_ID, success=False, error_message=str(err))
        logger.warning("paper_twin_star partial: intake=%s update=%s", intake, update)
        return
    insert_record(JOB_ID, success=True)
    logger.info(
        "paper_twin_star ok: %s intake +%s / skip %s · update closed %s",
        today,
        intake.get("inserted"),
        intake.get("skipped"),
        update.get("closed"),
    )
