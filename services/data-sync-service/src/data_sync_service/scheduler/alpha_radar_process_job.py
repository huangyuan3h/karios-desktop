"""Alpha Radar raw document processing (every 1 hour)."""

from __future__ import annotations

import logging
import os

from apscheduler.triggers.interval import IntervalTrigger  # type: ignore[import-not-found]

from data_sync_service.scheduler._job_guard import record_success, run_guarded
from data_sync_service.service.alpha_radar_pipeline import run_alpha_radar_process

logger = logging.getLogger(__name__)

JOB_ID = "alpha_radar_process_job"
DEFAULT_INTERVAL_HOURS = 1


def process_interval_hours() -> int:
    raw = os.getenv("ALPHA_RADAR_PROCESS_INTERVAL_HOURS", str(DEFAULT_INTERVAL_HOURS)).strip()
    try:
        return max(1, int(raw))
    except ValueError:
        return DEFAULT_INTERVAL_HOURS


def build_trigger():
    # OPT-108 (2026-08-13): LLM off-peak — 20:30 / 23:30 / 02:30 / 05:30
    # Asia/Shanghai (all inside the off-peak window 18:00-24:00 + 00:30-08:30,
    # user approved 19:00 起跑), instead of an every-1h IntervalTrigger.
    # The env override still works for manual tuning.
    from apscheduler.triggers.cron import CronTrigger  # type: ignore[import-not-found]

    if os.getenv("ALPHA_RADAR_PROCESS_NIGHTLY_CRON", "1") == "1":
        return CronTrigger(hour="20,23,2,5", minute="30", timezone="Asia/Shanghai")
    return IntervalTrigger(hours=process_interval_hours())


def run():
    logger.info("[alpha_radar] Starting scheduled raw process...")
    result = run_guarded(JOB_ID, lambda: run_alpha_radar_process(trigger="cron"), log=logger)
    if result is None:
        return  # exception path already recorded + logged
    logger.info(
        "[alpha_radar] Process complete: "
        f"processed={result.get('processedHeadlines')} "
        f"trends={result.get('trendsProduced')} "
        f"rounds={result.get('processRounds')} "
        f"raw_backlog={result.get('rawBacklogCount')}"
    )
    record_success(JOB_ID)
