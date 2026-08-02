"""News enrichment scheduled job — LLM-powered Track 2.

Runs after news_fetch_job to enrich newly ingested items with tickers,
sectors, event_type, importance, relevance_score, and ai_summary.
"""

from __future__ import annotations

import logging

from apscheduler.triggers.interval import IntervalTrigger  # type: ignore[import-not-found]

from data_sync_service.db.sync_job_record import insert_record
from data_sync_service.db.news import count_by_enrichment_status

logger = logging.getLogger(__name__)

JOB_ID = "news_enrich_job"


def build_trigger():
    # Run every 2 hours — lighter than fetch (4h), keeps items fresh
    return IntervalTrigger(hours=2)


def run():
    print("[news-enrich] Starting LLM enrichment cycle...")
    try:
        from data_sync_service.service.news_enrich import run_enrichment_cycle

        summary = run_enrichment_cycle(max_batches=10)
        status_counts = count_by_enrichment_status()
        success = summary["totalFailed"] == 0
        err_msg = None
        if summary["totalFailed"] > 0:
            err_msg = f"failed={summary['totalFailed']}; enriched={summary['totalEnriched']}"
        insert_record(
            JOB_ID,
            success=success,
            last_ts_code=str(summary["totalEnriched"]),
            error_message=err_msg,
        )
        print(
            f"[news-enrich] Done: batches={summary['batchesProcessed']} "
            f"enriched={summary['totalEnriched']} failed={summary['totalFailed']} "
            f"status={status_counts}"
        )
    except Exception as e:
        insert_record(JOB_ID, success=False, error_message=str(e))
        logger.warning("news_enrich_job failed: %s", e)
