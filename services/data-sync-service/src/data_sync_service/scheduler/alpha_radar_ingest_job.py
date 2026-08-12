"""Alpha Radar RSS-only ingest (every 4 hours)."""

from __future__ import annotations

import logging
import os

from apscheduler.triggers.interval import IntervalTrigger  # type: ignore[import-not-found]

from data_sync_service.service.alpha_radar_pipeline import run_alpha_radar_ingest

logger = logging.getLogger(__name__)

JOB_ID = "alpha_radar_ingest_job"
DEFAULT_INTERVAL_HOURS = 4


def ingest_interval_hours() -> int:
    raw = os.getenv("ALPHA_RADAR_INGEST_INTERVAL_HOURS", str(DEFAULT_INTERVAL_HOURS)).strip()
    try:
        return max(1, int(raw))
    except ValueError:
        return DEFAULT_INTERVAL_HOURS


def build_trigger():
    return IntervalTrigger(hours=ingest_interval_hours())


def run():
    logger.info("[alpha_radar] Starting scheduled RSS ingest...")
    try:
        result = run_alpha_radar_ingest(trigger="cron")
        stats = result.get("ingestStats") or {}
        logger.info(
            "[alpha_radar] Ingest complete: "
            f"stored={stats.get('stored')} "
            f"new={stats.get('new')} "
            f"requeued={stats.get('requeued')} "
            f"unchanged={stats.get('unchanged')} "
            f"raw_backlog={result.get('rawBacklogCount')}"
        )
    except Exception as exc:
        logger.warning(f"[alpha_radar] Ingest failed: {exc}")
