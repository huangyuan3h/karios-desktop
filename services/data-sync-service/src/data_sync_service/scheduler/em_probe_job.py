"""East Money egress probe — every 10min (OPT-126).

Independent `em_probe` job_type: probe failures never touch a business
`sync_job_record` (anti-pattern). 10min is fast enough to catch a ban
within the trading session and slow enough to never self-trigger one.
"""

from __future__ import annotations

import logging

from apscheduler.triggers.interval import IntervalTrigger  # type: ignore[import-not-found]

from data_sync_service.db.sync_job_record import insert_record
from data_sync_service.service.em_probe import JOB_TYPE, run_em_probe

logger = logging.getLogger(__name__)

JOB_ID = JOB_TYPE
INTERVAL_MINUTES = 10


def build_trigger() -> IntervalTrigger:
    return IntervalTrigger(minutes=INTERVAL_MINUTES)


def run() -> None:
    try:
        result = run_em_probe()
    except Exception as exc:  # noqa: BLE001
        insert_record(JOB_ID, success=False, error_message=str(exc))
        logger.warning("em_probe failed: %s", exc)
        return
    if result.get("failed"):
        insert_record(
            JOB_ID,
            success=False,
            error_message="probe failing: " + ",".join(result["failed"]),
        )
        return
    insert_record(JOB_ID, success=True)
    logger.info("em_probe ok: %s", [(c["host"], c["ms"]) for c in result.get("checks", [])])
