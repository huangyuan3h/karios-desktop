"""News RSS fetch scheduled job."""

from __future__ import annotations

from apscheduler.triggers.interval import IntervalTrigger  # type: ignore[import-not-found]

from data_sync_service.service.news import fetch_all_sources

JOB_ID = "news_fetch_job"


def build_trigger():
    return IntervalTrigger(hours=4)


def run():
    print("[news] Starting scheduled RSS fetch...")
    try:
        results = fetch_all_sources()
        print(f"[news] Fetch complete: {results}")
    except Exception as e:
        print(f"[news] Fetch failed: {e}")
