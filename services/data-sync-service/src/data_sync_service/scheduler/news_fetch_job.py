"""News RSS fetch scheduled job."""

from __future__ import annotations

import logging

from apscheduler.triggers.interval import IntervalTrigger  # type: ignore[import-not-found]

from data_sync_service.db.sync_job_record import insert_record
from data_sync_service.service.news import fetch_all_sources

logger = logging.getLogger(__name__)

JOB_ID = "news_fetch_job"


def build_trigger():
    return IntervalTrigger(hours=4)


def run():
    logger.info("[news] Starting scheduled RSS fetch...")
    try:
        results = fetch_all_sources()
        total = sum(int(v) for v in results.values() if isinstance(v, int) and v >= 0)
        failed = sum(1 for v in results.values() if isinstance(v, int) and v < 0)
        success = failed == 0
        err_msg = None
        if failed:
            err_msg = f"{failed} source(s) failed; fetched={total}"
        insert_record(
            JOB_ID,
            success=success,
            last_ts_code=str(total),
            error_message=err_msg,
        )
        logger.info(f"[news] Fetch complete: {results} (recorded success={success})")
    except Exception as e:
        insert_record(JOB_ID, success=False, error_message=str(e))
        logger.warning("news_fetch_job failed: %s", e)
