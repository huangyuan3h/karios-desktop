"""Alpha Radar scheduled pipeline job (every 12 hours).

2026-08-11: records every run (ok/skipped/failed) into sync_job_record so
the healthcheck's ``alpha_radar`` staleness reflects reality. Previously the
job only printed; combined with a 12h cooldown == 12h interval trigger, the
pipeline was permanently skipped since 2026-08-06 (cooldown bumped to 6h in
service/alpha_radar_pipeline.py).
"""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger  # type: ignore[import-not-found]

from data_sync_service.db.sync_job_record import insert_record
from data_sync_service.service.alpha_radar_pipeline import run_alpha_radar_pipeline

logger = logging.getLogger(__name__)

JOB_ID = "alpha_radar_pipeline_job"


def build_trigger():
    # OPT-108 (2026-08-13): LLM off-peak — fixed 19:30 Asia/Shanghai
    # (user-approved: off-peak window starts 18:00; 19:00 起跑), instead of
    # a process-start IntervalTrigger.
    return CronTrigger(hour="19", minute="30", timezone="Asia/Shanghai")


def run():
    logger.info("[alpha_radar] Starting scheduled 12h pipeline...")
    try:
        result = run_alpha_radar_pipeline(force=False, trigger="cron")
        if result.get("skipped"):
            logger.info(f"[alpha_radar] Pipeline skipped (cooldown): {result.get('lastRunAt')}")
            insert_record(JOB_ID, success=True, error_message="skipped-cooldown")
        elif result.get("ok"):
            stats = result.get("ingestStats", {})
            logger.info(
                "[alpha_radar] Pipeline complete: "
                f"stored={stats.get('stored')} trends={result.get('trendCount')}"
            )
            insert_record(
                JOB_ID,
                success=True,
                last_ts_code=str(result.get("trendCount") or 0),
                error_message=None,
            )
        else:
            logger.info(f"[alpha_radar] Pipeline failed: {result.get('errors')}")
            insert_record(JOB_ID, success=False, error_message=str(result.get("errors"))[:500])
    except Exception as exc:
        logger.warning(f"[alpha_radar] Pipeline failed: {exc}")
        insert_record(JOB_ID, success=False, error_message=str(exc)[:500])
