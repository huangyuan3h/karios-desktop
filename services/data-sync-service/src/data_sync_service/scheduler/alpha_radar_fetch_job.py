"""Alpha Radar scheduled pipeline job (every 12 hours).

2026-08-11: records every run (ok/skipped/failed) into sync_job_record so
the healthcheck's ``alpha_radar`` staleness reflects reality. Previously the
job only printed; combined with a 12h cooldown == 12h interval trigger, the
pipeline was permanently skipped since 2026-08-06 (cooldown bumped to 6h in
service/alpha_radar_pipeline.py).
"""

from __future__ import annotations

from apscheduler.triggers.interval import IntervalTrigger  # type: ignore[import-not-found]

from data_sync_service.db.sync_job_record import insert_record
from data_sync_service.service.alpha_radar_pipeline import run_alpha_radar_pipeline

JOB_ID = "alpha_radar_pipeline_job"


def build_trigger():
    return IntervalTrigger(hours=12)


def run():
    print("[alpha_radar] Starting scheduled 12h pipeline...")
    try:
        result = run_alpha_radar_pipeline(force=False, trigger="cron")
        if result.get("skipped"):
            print(f"[alpha_radar] Pipeline skipped (cooldown): {result.get('lastRunAt')}")
            insert_record(JOB_ID, success=True, error_message="skipped-cooldown")
        elif result.get("ok"):
            stats = result.get("ingestStats", {})
            print(
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
            print(f"[alpha_radar] Pipeline failed: {result.get('errors')}")
            insert_record(JOB_ID, success=False, error_message=str(result.get("errors"))[:500])
    except Exception as exc:
        print(f"[alpha_radar] Pipeline failed: {exc}")
        insert_record(JOB_ID, success=False, error_message=str(exc)[:500])
