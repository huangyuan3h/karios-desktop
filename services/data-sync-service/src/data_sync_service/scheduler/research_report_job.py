"""Research report sync scheduled job (研报 → Alpha channel).

Pulls recent East Money sell-side reports every 2h (dedup by info_code).
Scoring happens lazily inside build_research_catalyst_payload during the
watchlist automation run, so this job is purely ingestion.
"""

from __future__ import annotations

import logging

from apscheduler.triggers.interval import IntervalTrigger  # type: ignore[import-not-found]

from data_sync_service.db.sync_job_record import insert_record

logger = logging.getLogger(__name__)

JOB_ID = "research_report_sync"


def build_trigger():
    return IntervalTrigger(hours=2)


def run():
    print("[research] Syncing East Money research reports...")
    try:
        from data_sync_service.service.research import sync_research_reports

        summary = sync_research_reports(days=3, max_pages=3)
        success = bool(summary.get("ok"))
        insert_record(
            JOB_ID,
            success=success,
            last_ts_code=str(summary.get("inserted") or 0),
            error_message=summary.get("error"),
        )
        print(
            f"[research] Done: fetched={summary.get('fetched')} "
            f"inserted={summary.get('inserted')}"
        )
    except Exception as e:  # noqa: BLE001
        insert_record(JOB_ID, success=False, error_message=str(e))
        logger.warning("research_report_sync failed: %s", e)
