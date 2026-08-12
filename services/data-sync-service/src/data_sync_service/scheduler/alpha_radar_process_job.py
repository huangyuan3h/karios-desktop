"""Alpha Radar raw document processing (every 1 hour)."""

from __future__ import annotations

import logging
import os

from apscheduler.triggers.interval import IntervalTrigger  # type: ignore[import-not-found]

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
    return IntervalTrigger(hours=process_interval_hours())


def run():
    logger.info("[alpha_radar] Starting scheduled raw process...")
    try:
        result = run_alpha_radar_process(trigger="cron")
        logger.info(
            "[alpha_radar] Process complete: "
            f"processed={result.get('processedHeadlines')} "
            f"trends={result.get('trendsProduced')} "
            f"rounds={result.get('processRounds')} "
            f"raw_backlog={result.get('rawBacklogCount')}"
        )
    except Exception as exc:
        logger.warning(f"[alpha_radar] Process failed: {exc}")
