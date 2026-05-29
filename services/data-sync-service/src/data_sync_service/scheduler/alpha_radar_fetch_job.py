"""Alpha Radar scheduled pipeline job (every 12 hours)."""

from __future__ import annotations

from apscheduler.triggers.interval import IntervalTrigger  # type: ignore[import-not-found]

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
        elif result.get("ok"):
            print(
                "[alpha_radar] Pipeline complete: "
                f"stored={result.get('ingestStats', {}).get('stored')} "
                f"trends={result.get('trendCount')}"
            )
        else:
            print(f"[alpha_radar] Pipeline failed: {result.get('errors')}")
    except Exception as exc:
        print(f"[alpha_radar] Pipeline failed: {exc}")
